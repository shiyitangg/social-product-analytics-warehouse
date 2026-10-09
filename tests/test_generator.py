"""Tests for the synthetic generator. Uses CSV output so no parquet engine is needed."""
import glob, json, os
import pandas as pd
import pytest
import generate_data as g

CFG = g.Config(n_users=2500, days=12, seed=7)


@pytest.fixture(scope="module")
def raw(tmp_path_factory):
    out = str(tmp_path_factory.mktemp("raw"))
    stats = g.simulate(CFG, out, fmt="csv", verbose=False)
    return out, stats


def _events(out):
    files = glob.glob(f"{out}/events/ingest_date=*/*.csv")
    frames = []
    for f in files:
        df = pd.read_csv(f)
        df["ingest_date"] = f.split("ingest_date=")[1].split("/")[0]
        frames.append(df)
    return pd.concat(frames, ignore_index=True)


def test_raw_row_count_matches_ground_truth(raw):
    out, stats = raw
    assert len(_events(out)) == stats["raw_rows"]


def test_duplicates_are_exact_copies_by_event_id(raw):
    out, stats = raw
    ev = _events(out)
    assert len(ev) - ev["event_id"].nunique() == stats["duplicate_rows"]
    assert ev["event_id"].nunique() == stats["total_event_ids"]


def test_bad_rows_are_exactly_the_injected_ones(raw):
    out, stats = raw
    ev = _events(out).drop_duplicates("event_id")
    ts = pd.to_datetime(ev["event_ts"], utc=True)
    bad = ev["user_id"].isna() | ~ev["event_type"].isin(g.VALID_EVENT_TYPES) | (ts > pd.Timestamp("2027-01-01", tz="UTC"))
    assert int(bad.sum()) == stats["bad_rows"]
    assert stats["valid_distinct_events"] == stats["total_event_ids"] - stats["bad_rows"]


def test_late_events_exist_and_never_arrive_before_event_date(raw):
    out, stats = raw
    # first delivery of each event_id = earliest ingest_date (re-deliveries may come later)
    ev = _events(out).sort_values("ingest_date").drop_duplicates("event_id")
    ev = ev[ev["user_id"].notna() & ev["event_type"].isin(g.VALID_EVENT_TYPES)]
    ts = pd.to_datetime(ev["event_ts"], utc=True)
    ev = ev[ts < pd.Timestamp("2027-01-01", tz="UTC")]
    lag = (pd.to_datetime(ev["ingest_date"]).dt.tz_localize("UTC").dt.normalize()
           - pd.to_datetime(ev["event_ts"], utc=True).dt.normalize()).dt.days
    assert lag.min() >= 0 and lag.max() <= CFG.max_late_days
    assert (lag > 0).sum() > 0


def test_deterministic_for_same_seed(tmp_path):
    a = g.simulate(CFG, str(tmp_path / "a"), "csv", verbose=False)
    b = g.simulate(CFG, str(tmp_path / "b"), "csv", verbose=False)
    for k in ("raw_rows", "bad_rows", "duplicate_rows", "late_events", "content_rows"):
        assert a[k] == b[k]


def test_user_attributes_change_over_time(raw):
    out, _ = raw
    snaps = pd.concat([pd.read_csv(f).assign(d=f.split("snapshot_date=")[1][:10])
                       for f in sorted(glob.glob(f"{out}/users_snapshot/*/users.csv"))])
    nun = snaps.groupby("user_id")[["country", "device", "segment"]].nunique().max(axis=1)
    assert (nun > 1).sum() > 0                      # SCD2 has something to track


def test_ground_truth_declines_written(raw):
    out, _ = raw
    gt = pd.read_csv(f"{out}/ground_truth/planted_creator_declines.csv")
    assert len(gt) == CFG.n_declines and gt["decline_factor"].between(0.3, 0.6).all()


def test_event_types_reference_content_only_when_expected(raw):
    out, _ = raw
    ev = _events(out).drop_duplicates("event_id")
    ev = ev[ev["event_type"].isin(g.VALID_EVENT_TYPES)]
    assert ev.loc[ev["event_type"] == "app_open", "content_id"].isna().all()
    assert ev.loc[ev["event_type"] != "app_open", "content_id"].notna().all()
