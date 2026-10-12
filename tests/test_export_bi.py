import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from export_bi import select_list  # noqa: E402


def test_booleans_are_exported_as_text_true_false():
    sel = select_list([("event_date", "date"), ("is_provisional", "boolean"), ("dau", "bigint")])
    assert sel == '"event_date", "is_provisional"::text as "is_provisional", "dau"'
