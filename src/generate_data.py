"""Synthetic social-app data generator.

All data is SYNTHETIC and is labelled as such everywhere in this project. It is
generated so that the pipeline can be tested against *known* ground truth:
  - late-arriving events (delivered 1-3 days after event time)
  - duplicate deliveries of the same event_id
  - malformed rows (null user, unknown event type, impossible timestamp)
  - users whose attributes (country / device / segment) change over time
  - creators whose engagement declines from a known onset date

Raw layout written (object-storage style, Hive partitions):
  <out>/events/ingest_date=YYYY-MM-DD/events-from-YYYY-MM-DD.parquet
  <out>/users_snapshot/snapshot_date=YYYY-MM-DD/users.parquet   (daily full snapshot)
  <out>/content/ingest_date=YYYY-MM-DD/content.parquet
  <out>/ground_truth/injection_summary.json, planted_creator_declines.csv
    (ground_truth is NEVER read by the pipeline; only by evaluation scripts/tests)

Usage:
  python src/generate_data.py --out data/raw                      # full size (~10M+ events)
  python src/generate_data.py --out data/raw_small --users 3000 --days 10   # quick run
"""
from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass, asdict
from datetime import date, timedelta

import numpy as np
import pandas as pd

EVENT_TYPES = ["app_open", "content_view", "like", "comment", "share", "follow", "content_create"]
VALID_EVENT_TYPES = set(EVENT_TYPES)
COUNTRIES = np.array(["US", "IN", "BR", "GB", "DE", "ID", "MX", "OTHER"])
COUNTRY_P = np.array([0.40, 0.15, 0.10, 0.08, 0.06, 0.06, 0.05, 0.10])
DEVICES = np.array(["ios", "android", "web"])
DEVICE_P = np.array([0.45, 0.45, 0.10])
SEGMENTS = np.array(["new", "casual", "power", "dormant"])
SEG_NEW, SEG_CASUAL, SEG_POWER, SEG_DORMANT = 0, 1, 2, 3
P_ACTIVE = np.array([0.65, 0.35, 0.85, 0.05])      # P(user active on a given day) by segment
VIEW_LAMBDA = np.array([6.0, 6.0, 18.0, 3.0])      # mean content views per active user-day
HOUR_P = np.array([1, 1, 1, 1, 1, 2, 3, 5, 6, 6, 6, 7, 8, 7, 6, 6, 6, 7, 9, 10, 10, 8, 5, 3], dtype=float)
HOUR_P = HOUR_P / HOUR_P.sum()


@dataclass
class Config:
    n_users: int = 70_000
    days: int = 40
    start: str = "2026-08-01"
    seed: int = 42
    creator_share: float = 0.08
    n_communities: int = 200
    late_rate: float = 0.03       # share of events delivered 1-3 days late
    dup_rate: float = 0.005       # share of (valid) events delivered twice
    bad_rate: float = 0.002       # share of events that are malformed
    n_declines: int = 15          # creators with a planted engagement decline
    max_late_days: int = 3


def _to_ts(day: date, seconds: np.ndarray) -> pd.Series:
    """Seconds-since-midnight (UTC) -> tz-aware microsecond timestamps."""
    base = np.datetime64(day.isoformat(), "us")
    arr = base + (seconds.astype("int64") * 1_000_000).astype("timedelta64[us]")
    return pd.to_datetime(pd.Series(arr)).dt.tz_localize("UTC")


# Audience model: a view picks a creator-owned item with weight popularity * (freshness + EVERGREEN), where
# freshness decays with the item's age (days) and EVERGREEN is a constant "back-catalogue" share. Items stay
# eligible for POOL_DAYS. This gives each creator a stable audience, so a creator-level engagement decline is a
# real, detectable trend instead of being drowned by "did they post this week?" noise.
POOL_DAYS = 90          # items stay eligible for the whole simulation (back-catalogue keeps earning views)
SEED_MAX_AGE = 30       # pre-existing content is 1..30 days old at the start
EVERGREEN = 0.25
SEED_POSTS_PER_CREATOR = 8


def _seed_ts(start: date, day_offsets: np.ndarray, seconds: np.ndarray) -> pd.Series:
    """Per-row timestamps for seed content: start + day_offset days + seconds (UTC, microseconds)."""
    base = np.datetime64(start.isoformat(), "us")
    arr = base + ((day_offsets.astype("int64") * 86400 + seconds.astype("int64")) * 1_000_000).astype("timedelta64[us]")
    return pd.to_datetime(pd.Series(arr)).dt.tz_localize("UTC")


def _sample_seconds(rng: np.random.Generator, n: int) -> np.ndarray:
    hours = rng.choice(24, size=n, p=HOUR_P)
    return hours * 3600 + rng.integers(0, 3600, size=n)


def _write(df: pd.DataFrame, path: str, fmt: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if fmt == "parquet":
        df.to_parquet(path, index=False, engine="pyarrow", compression="snappy",
                      coerce_timestamps="us", allow_truncated_timestamps=True)
    else:  # csv is only used for tests / environments without pyarrow
        df.to_csv(path.replace(".parquet", ".csv"), index=False)


def simulate(cfg: Config, out: str, fmt: str = "parquet", verbose: bool = True) -> dict:
    rng = np.random.default_rng(cfg.seed)
    start = date.fromisoformat(cfg.start)
    N = cfg.n_users

    # ---------------- static user attributes ----------------
    uid_all = np.arange(1, N + 1, dtype="int64")
    in_window = rng.random(N) < 0.30                                  # 30% sign up during the window
    signup_day = np.where(in_window, rng.integers(0, cfg.days, N), -rng.integers(1, 366, N)).astype("int64")
    is_creator = rng.random(N) < cfg.creator_share
    creator_pop = np.where(is_creator, rng.lognormal(0.0, 1.2, N), 0.0)
    community = np.minimum(rng.zipf(1.6, N), cfg.n_communities) - 1   # skewed community sizes
    creator_idx = np.flatnonzero(is_creator)

    # ---------------- dynamic attributes (SCD2 source) ----------------
    country = rng.choice(len(COUNTRIES), size=N, p=COUNTRY_P)
    device = rng.choice(len(DEVICES), size=N, p=DEVICE_P)
    seg = np.where(signup_day > -7, SEG_NEW,
                   rng.choice([SEG_CASUAL, SEG_POWER, SEG_DORMANT], size=N, p=[0.55, 0.12, 0.33]))
    signup_date_obj = np.array([start + timedelta(days=int(d)) for d in signup_day], dtype=object)

    # ---------------- planted creator declines (ground truth) ----------------
    top = creator_idx[np.argsort(-creator_pop[creator_idx])][: max(1, int(len(creator_idx) * 0.15))]
    decl_users = rng.choice(top, size=min(cfg.n_declines, len(top)), replace=False)
    decl_onset = rng.integers(max(8, cfg.days // 2), max(9, cfg.days - 5), size=len(decl_users))
    decl_factor = rng.uniform(0.35, 0.55, size=len(decl_users))
    decline_onset_by_user = np.full(N, 10**6, dtype="int64")
    decline_factor_by_user = np.ones(N)
    decline_onset_by_user[decl_users] = decl_onset
    decline_factor_by_user[decl_users] = decl_factor

    # ---------------- content pool (seed content created before the window) ----------------
    n_seed = max(500, int(len(creator_idx) * SEED_POSTS_PER_CREATOR))
    seed_creators = rng.choice(creator_idx, size=n_seed)
    c_ids = list(range(1, n_seed + 1))
    c_creator = list(seed_creators + 1)
    c_comm = list(community[seed_creators])
    c_day = list(-rng.integers(1, SEED_MAX_AGE + 1, size=n_seed))
    c_type = list(rng.choice(["video", "post", "story"], size=n_seed, p=[0.45, 0.40, 0.15]))
    seed_content = pd.DataFrame({
        "content_id": np.array(c_ids, dtype="int64"), "creator_id": np.array(c_creator, dtype="int64"),
        "community_id": np.array(c_comm, dtype="int64"), "content_type": np.array(c_type),
        "created_ts": _seed_ts(start, np.array(c_day), _sample_seconds(rng, n_seed)),
    })
    _write(seed_content, f"{out}/content/ingest_date={start.isoformat()}/content-seed.parquet", fmt)
    next_cid = n_seed + 1

    pool_id = np.array(c_ids, dtype="int64")
    pool_creator = np.array(c_creator, dtype="int64")
    pool_day = np.array(c_day, dtype="int64")

    next_eid = 1
    stats = dict(total_event_ids=0, bad_rows=0, bad_null_user=0, bad_unknown_type=0, bad_future_ts=0,
                 duplicate_rows=0, late_events=0, raw_rows=0, valid_distinct_events=0, content_rows=n_seed)
    per_day_valid = {}

    for d in range(cfg.days):
        day = start + timedelta(days=d)

        # ---- user attribute dynamics (state at the start of day d) ----
        exists = signup_day <= d
        if d > 0:
            prev = seg.copy()
            r = rng.random(N)
            seg[(prev == SEG_NEW) & (d - signup_day >= 7)] = SEG_CASUAL
            seg[(prev == SEG_CASUAL) & (r < 0.003)] = SEG_POWER
            seg[(prev == SEG_POWER) & (r < 0.002)] = SEG_CASUAL
            seg[(prev == SEG_CASUAL) & (r >= 0.003) & (r < 0.006)] = SEG_DORMANT
            seg[(prev == SEG_DORMANT) & (r < 0.005)] = SEG_CASUAL
            chg_c = exists & (rng.random(N) < 0.0005)
            country[chg_c] = (country[chg_c] + rng.integers(1, len(COUNTRIES), chg_c.sum())) % len(COUNTRIES)
            chg_d = exists & (rng.random(N) < 0.001)
            device[chg_d] = (device[chg_d] + rng.integers(1, len(DEVICES), chg_d.sum())) % len(DEVICES)
        snap = pd.DataFrame({
            "user_id": uid_all[exists], "signup_date": signup_date_obj[exists],
            "country": COUNTRIES[country[exists]], "device": DEVICES[device[exists]],
            "segment": SEGMENTS[seg[exists]], "is_creator": is_creator[exists],
        })
        _write(snap, f"{out}/users_snapshot/snapshot_date={day.isoformat()}/users.parquet", fmt)

        # ---- who is active today ----
        weekly = 1.0 + 0.10 * np.sin(2 * np.pi * d / 7.0)
        active = exists & (rng.random(N) < np.minimum(1.0, P_ACTIVE[seg] * weekly))
        a_idx = np.flatnonzero(active)
        a_uid = a_idx + 1

        parts_u, parts_s, parts_t, parts_c = [], [], [], []

        # app_open: 1 + Poisson(0.8) sessions per active user
        n_open = 1 + rng.poisson(0.8, len(a_idx))
        u = np.repeat(a_uid, n_open)
        parts_u.append(u); parts_s.append(_sample_seconds(rng, len(u)))
        parts_t.append(np.zeros(len(u), dtype="int8")); parts_c.append(np.full(len(u), -1, dtype="int64"))

        # content_view against the pool of already-created content
        age = d - pool_day
        keep = (age >= 1) & (age <= POOL_DAYS)
        k_id, k_creator, k_age = pool_id[keep], pool_creator[keep], age[keep]
        decl_mult = np.where(d >= decline_onset_by_user[k_creator - 1], decline_factor_by_user[k_creator - 1], 1.0)
        w = creator_pop[k_creator - 1] * (np.exp(-k_age / 3.0) + EVERGREEN) * decl_mult
        cw = np.cumsum(w); cw /= cw[-1]
        n_view = rng.poisson(VIEW_LAMBDA[seg[a_idx]])
        vu = np.repeat(a_uid, n_view)
        vs = _sample_seconds(rng, len(vu))
        vc = k_id[np.minimum(np.searchsorted(cw, rng.random(len(vu))), len(k_id) - 1)]
        parts_u.append(vu); parts_s.append(vs); parts_t.append(np.full(len(vu), 1, dtype="int8")); parts_c.append(vc)

        # interactions follow views
        for type_code, p in ((2, 0.10), (3, 0.025), (4, 0.012), (5, 0.010)):
            m = rng.random(len(vu)) < p
            parts_u.append(vu[m]); parts_s.append(np.minimum(vs[m] + rng.integers(1, 120, m.sum()), 86399))
            parts_t.append(np.full(m.sum(), type_code, dtype="int8")); parts_c.append(vc[m])

        # content_create
        is_c = is_creator[a_idx]
        rate = np.where(is_c, 0.7 * np.where(seg[a_idx] == SEG_POWER, 2.0, 1.0)
                        * np.where(d >= decline_onset_by_user[a_idx], decline_factor_by_user[a_idx], 1.0), 0.01)
        n_new = rng.poisson(rate)
        cu = np.repeat(a_uid, n_new)
        if len(cu):
            cs = _sample_seconds(rng, len(cu))
            new_ids = np.arange(next_cid, next_cid + len(cu), dtype="int64"); next_cid += len(cu)
            ctype = rng.choice(["video", "post", "story"], size=len(cu), p=[0.45, 0.40, 0.15])
            cc = np.where(rng.random(len(cu)) < 0.85, community[cu - 1], rng.integers(0, cfg.n_communities, len(cu)))
            content_df = pd.DataFrame({"content_id": new_ids, "creator_id": cu, "community_id": cc.astype("int64"),
                                       "content_type": ctype, "created_ts": _to_ts(day, cs)})
            _write(content_df, f"{out}/content/ingest_date={day.isoformat()}/content.parquet", fmt)
            stats["content_rows"] += len(cu)
            parts_u.append(cu); parts_s.append(cs); parts_t.append(np.full(len(cu), 6, dtype="int8")); parts_c.append(new_ids)
            pool_id = np.concatenate([pool_id, new_ids]); pool_creator = np.concatenate([pool_creator, cu])
            pool_day = np.concatenate([pool_day, np.full(len(cu), d, dtype="int64")])
        # drop expired content from the pool to keep arrays small
        alive = (d - pool_day) <= POOL_DAYS
        pool_id, pool_creator, pool_day = pool_id[alive], pool_creator[alive], pool_day[alive]

        u = np.concatenate(parts_u); s = np.concatenate(parts_s); t = np.concatenate(parts_t); c = np.concatenate(parts_c)
        order = np.argsort(s, kind="stable")
        u, s, t, c = u[order], s[order], t[order], c[order]
        n = len(u)
        ev = pd.DataFrame({
            "event_id": np.arange(next_eid, next_eid + n, dtype="int64"),
            "event_ts": _to_ts(day, s),
            "user_id": pd.array(u, dtype="Int64"),
            "event_type": np.array(EVENT_TYPES)[t],
            "content_id": pd.array(np.where(c < 0, 0, c), dtype="Int64"),
        })
        ev.loc[c < 0, "content_id"] = pd.NA
        next_eid += n

        # ---- inject malformed rows ----
        bad = rng.random(n) < cfg.bad_rate
        kind = rng.integers(0, 3, n)
        m0, m1, m2 = bad & (kind == 0), bad & (kind == 1), bad & (kind == 2)
        ev.loc[m0, "user_id"] = pd.NA
        ev.loc[m1, "event_type"] = "bogus_event"
        ev.loc[m2, "event_ts"] = ev.loc[m2, "event_ts"] + pd.Timedelta(days=400)
        stats["bad_rows"] += int(bad.sum()); stats["bad_null_user"] += int(m0.sum())
        stats["bad_unknown_type"] += int(m1.sum()); stats["bad_future_ts"] += int(m2.sum())

        # ---- late arrival + duplicates (duplicates only copy valid rows) ----
        lag = np.where(rng.random(n) < cfg.late_rate, rng.integers(1, cfg.max_late_days + 1, n), 0)
        stats["late_events"] += int((lag > 0).sum())
        dup = (~bad) & (rng.random(n) < cfg.dup_rate)
        dup_extra = rng.integers(0, 3, int(dup.sum()))
        dups = ev[dup].copy()
        ev = ev.assign(ingest_date=[(day + timedelta(days=int(x))).isoformat() for x in lag])
        dups = dups.assign(ingest_date=[(day + timedelta(days=int(l + e))).isoformat()
                                        for l, e in zip(lag[dup], dup_extra)])
        allrows = pd.concat([ev, dups], ignore_index=True)
        stats["duplicate_rows"] += len(dups)
        stats["total_event_ids"] += n
        stats["raw_rows"] += len(allrows)
        per_day_valid[day.isoformat()] = int(n - bad.sum())

        for ing, grp in allrows.groupby("ingest_date"):
            _write(grp.drop(columns=["ingest_date"]),
                   f"{out}/events/ingest_date={ing}/events-from-{day.isoformat()}.parquet", fmt)
        if verbose and (d % 5 == 0 or d == cfg.days - 1):
            print(f"  day {d + 1:>2}/{cfg.days} {day}  events={n:,}  DAU={len(a_idx):,}")

    stats["valid_distinct_events"] = stats["total_event_ids"] - stats["bad_rows"]
    stats["config"] = asdict(cfg)
    stats["note"] = "SYNTHETIC DATA. Ground truth for evaluation only; never read by the pipeline."
    gt = f"{out}/ground_truth"
    os.makedirs(gt, exist_ok=True)
    json.dump(stats, open(f"{gt}/injection_summary.json", "w"), indent=2)
    json.dump(per_day_valid, open(f"{gt}/valid_events_by_event_date.json", "w"), indent=2)
    rank = {int(u): i + 1 for i, u in enumerate(creator_idx[np.argsort(-creator_pop[creator_idx])] + 1)}
    pd.DataFrame({
        "creator_id": decl_users + 1,
        "onset_date": [(start + timedelta(days=int(x))).isoformat() for x in decl_onset],
        "decline_factor": np.round(decl_factor, 3),
        "popularity_rank": [rank[int(u) + 1] for u in decl_users],
    }).sort_values("onset_date").to_csv(f"{gt}/planted_creator_declines.csv", index=False)
    return stats


def main() -> None:
    ap = argparse.ArgumentParser(description="Generate SYNTHETIC social-app raw data")
    ap.add_argument("--out", default="data/raw")
    ap.add_argument("--users", type=int, default=Config.n_users)
    ap.add_argument("--days", type=int, default=Config.days)
    ap.add_argument("--start", default=Config.start)
    ap.add_argument("--seed", type=int, default=Config.seed)
    ap.add_argument("--format", choices=["parquet", "csv"], default="parquet")
    a = ap.parse_args()
    cfg = Config(n_users=a.users, days=a.days, start=a.start, seed=a.seed)
    print(f"Generating SYNTHETIC data -> {a.out}  (users={cfg.n_users:,}, days={cfg.days})")
    s = simulate(cfg, a.out, a.format)
    print(json.dumps({k: v for k, v in s.items() if k != "config"}, indent=2))


if __name__ == "__main__":
    main()
