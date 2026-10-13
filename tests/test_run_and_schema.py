"""Runner and schema tests: stale handling, drop guard, and arithmetic checks."""

from __future__ import annotations

import json
import sys
import types

import pytest

from pipeline import run
from pipeline.schema import Row, SourceError, read_rows, validate_rows, write_rows


def rows_for(as_of: str, total: int, mail: int | None = None):
    mail = total if mail is None else mail
    early = total - mail
    out = []
    for county in ("ALL", "WAKE"):
        for method, n in (("mail", mail), ("early_in_person", early), ("all", total)):
            out.append(Row("ZZ", county, as_of, method, "total", "all", n))
            out.append(Row("ZZ", county, as_of, method, "party", "DEM", n))
            out.append(Row("ZZ", county, as_of, method, "age", "18-25", n))
            out.append(Row("ZZ", county, as_of, method, "race", "WHITE", n))
            out.append(Row("ZZ", county, as_of, method, "ethnicity", "UNDESIGNATED", n))
            out.append(Row("ZZ", county, as_of, method, "gender", "FEMALE", n))
            out.append(Row("ZZ", county, as_of, method, "return_date", "2026-10-15", n))
    return out


class FakeResult:
    def __init__(self, as_of, total):
        self.as_of_date = as_of
        self.rows = rows_for(as_of, total)
        self.meta = {"as_of_date": as_of, "source_version": f"v-{as_of}-{total}", "warnings": []}


@pytest.fixture
def sandbox(tmp_path, monkeypatch):
    monkeypatch.setattr(run, "ROOT", tmp_path)
    monkeypatch.setattr(run, "SNAPSHOTS", tmp_path / "data" / "snapshots")
    monkeypatch.setattr(run, "STATUS_PATH", tmp_path / "data" / "status.json")
    module = types.ModuleType("fake_state")
    module.NAME = "Fakeland"
    module.next = None
    module.source_version = lambda: module.next.meta["source_version"] if isinstance(module.next, FakeResult) else "x"

    def scrape():
        if isinstance(module.next, Exception):
            raise module.next
        return module.next

    module.scrape = scrape
    monkeypatch.setitem(sys.modules, "fake_state", module)
    monkeypatch.setattr(run, "STATES", {"ZZ": "fake_state"})
    return tmp_path, module


def status(tmp_path):
    return json.loads((tmp_path / "data" / "status.json").read_text())["states"]["ZZ"]


def test_success_writes_snapshot(sandbox):
    tmp, mod = sandbox
    mod.next = FakeResult("2026-10-20", 100)
    assert run.main(["--force"]) == 0
    assert (tmp / "data/snapshots/zz/2026-10-20.csv").exists()
    s = status(tmp)
    assert s["ok"] and s["good"]["statewide_total"] == 100


def test_failure_keeps_last_good_snapshot_and_marks_stale(sandbox):
    tmp, mod = sandbox
    mod.next = FakeResult("2026-10-20", 100)
    run.main(["--force"])
    before = (tmp / "data/snapshots/zz/2026-10-20.csv").read_text()

    mod.next = SourceError("record file header changed")
    assert run.main(["--force"]) == 1
    assert (tmp / "data/snapshots/zz/2026-10-20.csv").read_text() == before
    assert sorted(p.name for p in (tmp / "data/snapshots/zz").iterdir()) == ["2026-10-20.csv"]
    s = status(tmp)
    assert s["ok"] is False
    assert "header changed" in s["last_error"]
    assert s["good"]["as_of_date"] == "2026-10-20"  # last good data is still described


def test_unexpected_exception_is_contained(sandbox):
    tmp, mod = sandbox
    mod.next = KeyError("boom")
    assert run.main(["--force"]) == 1
    assert status(tmp)["last_error"].startswith("KeyError")


def test_large_drop_fails(sandbox):
    tmp, mod = sandbox
    mod.next = FakeResult("2026-10-20", 1000)
    run.main(["--force"])
    mod.next = FakeResult("2026-10-21", 900)
    assert run.main(["--force"]) == 1
    assert "fell from 1,000 to 900" in status(tmp)["last_error"]
    assert not (tmp / "data/snapshots/zz/2026-10-21.csv").exists()


def test_small_drop_allowed(sandbox):
    tmp, mod = sandbox
    mod.next = FakeResult("2026-10-20", 1000)
    run.main(["--force"])
    mod.next = FakeResult("2026-10-21", 995)
    assert run.main(["--force"]) == 0


def test_older_data_fails(sandbox):
    tmp, mod = sandbox
    mod.next = FakeResult("2026-10-20", 100)
    run.main(["--force"])
    mod.next = FakeResult("2026-10-19", 100)
    assert run.main(["--force"]) == 1
    assert "older than the last snapshot" in status(tmp)["last_error"]


def test_unchanged_source_is_not_a_failure(sandbox):
    tmp, mod = sandbox
    mod.next = FakeResult("2026-10-20", 100)
    run.main(["--force"])
    assert run.main(["--force"]) == 0
    assert status(tmp)["last_result"] == "unchanged"


def test_stops_after_election(sandbox, monkeypatch):
    import datetime as dt

    class FrozenDateTime(dt.datetime):
        @classmethod
        def now(cls, tz=None):
            return dt.datetime(2026, 11, 10, 12, tzinfo=tz)

    monkeypatch.setattr(run.dt, "datetime", FrozenDateTime)
    assert run.main([]) == 0
    assert not (sandbox[0] / "data/status.json").exists()


def test_validate_catches_breakdown_mismatch():
    rows = rows_for("2026-10-20", 10)
    rows = [r if not (r.county == "WAKE" and r.method == "all" and r.group_type == "age")
            else Row(r.state, r.county, r.as_of_date, r.method, r.group_type, r.group_value, 9) for r in rows]
    with pytest.raises(SourceError, match="age breakdown sums to 9"):
        validate_rows(rows)


def test_validate_catches_methods_not_summing():
    rows = [r for r in rows_for("2026-10-20", 10, mail=4)]
    rows = [Row(r.state, r.county, r.as_of_date, r.method, r.group_type, r.group_value, r.ballots + 1)
            if r.method == "mail" else r for r in rows]
    with pytest.raises(SourceError, match="methods sum"):
        validate_rows(rows)


def test_validate_rejects_duplicates_and_bad_values():
    good = rows_for("2026-10-20", 3)
    with pytest.raises(SourceError, match="duplicate"):
        validate_rows(good + good[:1])
    with pytest.raises(SourceError, match="unknown method"):
        validate_rows([Row("ZZ", "ALL", "2026-10-20", "carrier", "total", "all", 1)])
    with pytest.raises(SourceError, match="bad ballot count"):
        validate_rows([Row("ZZ", "ALL", "2026-10-20", "all", "total", "all", -1)])


def test_csv_round_trip(tmp_path):
    rows = rows_for("2026-10-20", 5)
    write_rows(tmp_path / "x.csv", rows)
    assert sorted(read_rows(tmp_path / "x.csv")) == sorted(rows)
