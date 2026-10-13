"""NC scraper tests. Fixtures are synthetic rows built with NCSBE's real header; no real voter data."""

from __future__ import annotations

import csv
import datetime as dt
import io
import zipfile
from pathlib import Path

import pytest

from pipeline import http
from pipeline.schema import STATEWIDE, SourceError, validate_rows
from pipeline.sources import nc

AS_OF = dt.datetime(2026, 10, 20, 9, 15, tzinfo=dt.timezone.utc)  # 5:15am ET on Oct 20


def record(county="WAKE", req_type="MAIL", status="ACCEPTED", returned="10/15/2026", party="DEM",
           race="WHITE", ethnicity="NOT HISPANIC or NOT LATINO", gender="F", age="44", election="11/03/2026"):
    row = {name: "" for name in nc.RECORD_HEADER}
    row.update(county_desc=county, ballot_req_type=req_type, ballot_rtn_status=status, ballot_rtn_dt=returned,
               voter_party_code=party, race=race, ethnicity=ethnicity, gender=gender, age=age, election_dt=election,
               voter_last_name="TESTER", voter_street_address="1 SYNTHETIC ST")
    return row


def make_zip(path: Path, rows, header=nc.RECORD_HEADER) -> Path:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(header)
    for r in rows:
        w.writerow([r.get(h, "") for h in header])
    with zipfile.ZipFile(path, "w") as z:
        z.writestr("absentee_20261103.csv", buf.getvalue())
    return path


def make_counts(by_type: dict[str, int]) -> str:
    """A full cross-tab counts file whose totals per request type are given."""
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(nc.STATE_COUNTS_HEADER)
    types = ["CIVILIAN", "EARLY VOTING", "MILITARY", "ONE-STOP", "OVERSEAS"]
    for t in types:
        for party in ["DEM", "REP"]:
            for gender in ["FEMALE", "MALE"]:
                n = by_type.get(t, 0) if (party, gender) == ("DEM", "FEMALE") else 0
                w.writerow([t, party, gender, "WHITE", "NOT HISPANIC or NOT LATINO", n])
    return out.getvalue()


def tally(rows, tmp_path, cutoff=dt.date(2026, 10, 19)):
    path = make_zip(tmp_path / "r.zip", rows)
    return nc.tally_records(nc.iter_records(path, election=nc.ELECTION_DATE), election=nc.ELECTION_DATE,
                            cutoff=cutoff)


def test_age_bands_match_ncsbe():
    assert nc.age_band("18") == "18-25"
    assert nc.age_band("25") == "18-25"
    assert nc.age_band("26") == "26-40"
    assert nc.age_band("65") == "41-65"
    assert nc.age_band("66") == "Over 65"
    assert nc.age_band("") == nc.UNKNOWN_AGE
    assert nc.age_band("269") == nc.UNKNOWN_AGE


def test_a_few_bad_ages_are_unknown_but_many_fail(tmp_path):
    t = tally([record()] * 999 + [record(age="269")], tmp_path)
    assert t.accepted == 1000
    with pytest.raises(SourceError, match="impossible age"):
        tally([record()] * 9 + [record(age="1957")], tmp_path)


def test_aggregates_and_balances(tmp_path):
    rows = [
        record(),
        record(county="DURHAM", req_type="ONE-STOP", party="UNA", gender="M", age="30", returned="10/16/2026"),
        record(county="DURHAM", req_type="ONE-STOP", party="REP", age="70", returned="10/16/2026"),
        record(status="SPOILED"),
        record(status="PENDING CURE"),
    ]
    t = tally(rows, tmp_path)
    assert t.accepted == 3
    out = validate_rows(nc.tally_to_rows(t, as_of_date="2026-10-20"), require_breakdowns=("party", "age"))
    get = {(r.county, r.method, r.group_type, r.group_value): r.ballots for r in out}
    assert get[(STATEWIDE, "all", "total", "all")] == 3
    assert get[(STATEWIDE, "early_in_person", "total", "all")] == 2
    assert get[("DURHAM", "all", "party", "UNA")] == 1
    assert get[(STATEWIDE, "all", "return_date", "2026-10-16")] == 2
    assert get[(STATEWIDE, "all", "gender", "MALE")] == 1
    # Every county gets a total row, even with zero ballots.
    assert get[("YANCEY", "all", "total", "all")] == 0


def test_publication_day_ballots_wait_for_next_edition(tmp_path):
    t = tally([record(returned="10/19/2026"), record(returned="10/20/2026")], tmp_path)
    assert t.accepted == 1
    assert t.accepted_including_cutoff == 2
    assert any("publication day" in w for w in t.warnings)


def test_header_change_fails(tmp_path):
    header = list(nc.RECORD_HEADER)
    header[header.index("ballot_rtn_dt")] = "ballot_return_date"
    path = make_zip(tmp_path / "r.zip", [], header=header)
    with pytest.raises(SourceError, match="header changed"):
        list(nc.iter_records(path, election=nc.ELECTION_DATE))


def test_wrong_election_fails(tmp_path):
    with pytest.raises(SourceError, match="election"):
        tally([record(election="11/08/2022")], tmp_path)


@pytest.mark.parametrize("field, value, message", [
    ("county", "ATLANTIS", "unknown county"),
    ("req_type", "CARRIER PIGEON", "unknown ballot_req_type"),
    ("returned", "01/01/2020", "outside"),
    ("returned", "not a date", "bad date"),
])
def test_bad_values_fail(tmp_path, field, value, message):
    with pytest.raises(SourceError, match=message):
        tally([record(**{field: value})], tmp_path)


def test_new_demographic_values_warn_but_count(tmp_path):
    t = tally([record(party="NEW")], tmp_path)
    assert t.accepted == 1
    assert any("party" in w for w in t.warnings)


def test_state_counts_parses_and_sums():
    totals = nc.parse_state_counts(make_counts({"CIVILIAN": 5, "ONE-STOP": 7, "MILITARY": 1}))
    assert totals == {"mail": 6, "early_in_person": 7}


def test_state_counts_partial_file_fails():
    text = make_counts({"CIVILIAN": 5})
    truncated = "\n".join(text.splitlines()[:-3]) + "\n"
    with pytest.raises(SourceError, match="cross-tab"):
        nc.parse_state_counts(truncated)


def test_state_counts_header_change_fails():
    lines = make_counts({"CIVILIAN": 5}).splitlines()
    lines[0] = lines[0].replace(",0", ",ballot_count")
    text = "\n".join(lines) + "\n"
    with pytest.raises(SourceError, match="header changed"):
        nc.parse_state_counts(text)


def _fake_http(monkeypatch, tmp_path, rows, counts_text, counts_time=AS_OF):
    zip_src = make_zip(tmp_path / "src.zip", rows)

    def download(url, dest):
        dest.write_bytes(zip_src.read_bytes())
        return http.Meta(url, AS_OF, "etag1", None)

    monkeypatch.setattr(nc.http, "download", download)
    monkeypatch.setattr(nc.http, "get_text", lambda url, encoding="utf-8": (
        counts_text, http.Meta(url, counts_time, "etag2", None)))


def test_scrape_end_to_end(monkeypatch, tmp_path):
    rows = [record(), record(req_type="ONE-STOP", returned="10/19/2026")]
    _fake_http(monkeypatch, tmp_path, rows, make_counts({"CIVILIAN": 1, "ONE-STOP": 1}))
    result = nc.scrape()
    assert result.as_of_date == "2026-10-20"
    assert result.meta["complete_through"] == "2026-10-19"
    assert result.meta["crosscheck"]["status"] == "matched"
    validate_rows(result.rows)


def test_scrape_fails_when_counts_disagree(monkeypatch, tmp_path):
    rows = [record() for _ in range(10)]
    _fake_http(monkeypatch, tmp_path, rows, make_counts({"CIVILIAN": 50}))
    with pytest.raises(SourceError, match="counts file says 50"):
        nc.scrape()


def test_crosscheck_skipped_when_files_are_from_different_runs(monkeypatch, tmp_path):
    rows = [record() for _ in range(10)]
    _fake_http(monkeypatch, tmp_path, rows, make_counts({"CIVILIAN": 50}),
               counts_time=AS_OF - dt.timedelta(days=1))
    result = nc.scrape()
    assert result.meta["crosscheck"]["status"] == "skipped"
