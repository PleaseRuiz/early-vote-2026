"""North Carolina: NC State Board of Elections absentee and early-voting files.

Primary source: the record-level absentee file (one row per ballot), published
daily as a ZIP on NCSBE's public S3 bucket. It includes voters' names and
addresses; this module reads only the columns it needs, one row at a time,
and never writes record-level data anywhere.

Cross-check: NCSBE's statewide counts file, which NCSBE computes separately.
Its total must match the number of accepted ballots we count in the ZIP.
"""

from __future__ import annotations

import csv
import datetime as dt
import io
import tempfile
import zipfile
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator
from zoneinfo import ZoneInfo

from pipeline import http
from pipeline.schema import Row, SourceError, STATEWIDE, TOTAL_VALUE, marginalize

STATE = "NC"
NAME = "North Carolina"
ELECTION_DATE = dt.date(2026, 11, 3)
EASTERN = ZoneInfo("America/New_York")

BASE_URL = "https://s3.amazonaws.com/dl.ncsbe.gov/ENRS/2026_11_03"
RECORDS_URL = f"{BASE_URL}/absentee_20261103.zip"
STATE_COUNTS_URL = f"{BASE_URL}/absentee_counts_state_20261103.csv"
SOURCE_PAGE = "https://www.ncsbe.gov/results-data/absentee-data"

# Observed header of absentee_YYYYMMDD.csv (identical in 2022 and 2026).
RECORD_HEADER = (
    "county_desc", "voter_reg_num", "ncid", "voter_last_name", "voter_first_name",
    "voter_middle_name", "race", "ethnicity", "gender", "age", "voter_street_address",
    "voter_city", "voter_state", "voter_zip", "ballot_mail_street_address", "ballot_mail_city",
    "ballot_mail_state", "ballot_mail_zip", "other_mail_addr1", "other_mail_addr2",
    "other_city_state_zip", "relative_request_name", "relative_request_address",
    "relative_request_city", "relative_request_state", "relative_request_zip", "election_dt",
    "voter_party_code", "precinct_desc", "cong_dist_desc", "nc_house_desc", "nc_senate_desc",
    "ballot_req_delivery_type", "ballot_req_type", "ballot_request_party", "ballot_req_dt",
    "ballot_send_dt", "ballot_rtn_dt", "ballot_rtn_status", "site_name", "sdr", "mail_veri_status",
)
COL = {name: i for i, name in enumerate(RECORD_HEADER)}

# Observed header of absentee_counts_state_YYYYMMDD.csv in 2026. The count
# column really is named "0".
STATE_COUNTS_HEADER = ("abs_request_type", "party_cd", "gender", "race", "ethnicity", "0")

COUNTIES = (
    "ALAMANCE", "ALEXANDER", "ALLEGHANY", "ANSON", "ASHE", "AVERY", "BEAUFORT", "BERTIE", "BLADEN",
    "BRUNSWICK", "BUNCOMBE", "BURKE", "CABARRUS", "CALDWELL", "CAMDEN", "CARTERET", "CASWELL",
    "CATAWBA", "CHATHAM", "CHEROKEE", "CHOWAN", "CLAY", "CLEVELAND", "COLUMBUS", "CRAVEN",
    "CUMBERLAND", "CURRITUCK", "DARE", "DAVIDSON", "DAVIE", "DUPLIN", "DURHAM", "EDGECOMBE",
    "FORSYTH", "FRANKLIN", "GASTON", "GATES", "GRAHAM", "GRANVILLE", "GREENE", "GUILFORD",
    "HALIFAX", "HARNETT", "HAYWOOD", "HENDERSON", "HERTFORD", "HOKE", "HYDE", "IREDELL", "JACKSON",
    "JOHNSTON", "JONES", "LEE", "LENOIR", "LINCOLN", "MACON", "MADISON", "MARTIN", "MCDOWELL",
    "MECKLENBURG", "MITCHELL", "MONTGOMERY", "MOORE", "NASH", "NEW HANOVER", "NORTHAMPTON",
    "ONSLOW", "ORANGE", "PAMLICO", "PASQUOTANK", "PENDER", "PERQUIMANS", "PERSON", "PITT", "POLK",
    "RANDOLPH", "RICHMOND", "ROBESON", "ROCKINGHAM", "ROWAN", "RUTHERFORD", "SAMPSON", "SCOTLAND",
    "STANLY", "STOKES", "SURRY", "SWAIN", "TRANSYLVANIA", "TYRRELL", "UNION", "VANCE", "WAKE",
    "WARREN", "WASHINGTON", "WATAUGA", "WAYNE", "WILKES", "WILSON", "YADKIN", "YANCEY",
)
COUNTY_SET = frozenset(COUNTIES)
assert len(COUNTIES) == 100

# Record file: ballot_req_type → method. Anything else fails the run.
RECORD_METHODS = {"MAIL": "mail", "ONE-STOP": "early_in_person", "EARLY VOTING": "early_in_person"}
# Counts file: abs_request_type → method. Military and overseas ballots are mail ballots.
COUNTS_METHODS = {
    "CIVILIAN": "mail", "MILITARY": "mail", "OVERSEAS": "mail",
    "ONE-STOP": "early_in_person", "EARLY VOTING": "early_in_person",
}

ACCEPTED = frozenset({"ACCEPTED", "ACCEPTED - EXCEPTION", "ACCEPTED - CURED"})
# Statuses seen in 2022 and 2026 files. An unseen status is recorded as a
# warning; the counts-file cross-check guards the definition of "accepted".
KNOWN_OTHER_STATUSES = frozenset({
    "", "PENDING CURE", "AFFIDAVIT REVIEW PENDING", "PHOTO ID CURABLE", "CONFLICT", "ID NOT PROVIDED",
    "SPOILED", "WITNESS INFO INCOMPLETE", "CANCELLED", "RETURNED AFTER DEADLINE",
    "RETURNED UNDELIVERABLE", "WRONG VOTER", "NOT VOTED", "DUPLICATE", "NOT PROVIDED",
    "SIGNATURE DIFFERENT", "VOTER SIGNATURE MISSING", "WITNESS SIGNATURE MISSING",
    "UNDELIVERABLE", "E-TRANSMISSION RETURN", "REJECTED", "VOTER DECEASED", "UNVERIFIED",
    "ASSISTANT INFO INCOMPLETE", "E-TRANSMISSION FAILURE", "NO TIME FOR CURE - CONTACTED",
    "NOT PROPERLY NOTARIZED", "PENDING",
})

KNOWN_PARTIES = frozenset({"DEM", "REP", "UNA", "LIB", "GRE", "CST", "JFA", "NLB", "WTP"})
KNOWN_RACES = frozenset({
    "WHITE", "BLACK or AFRICAN AMERICAN", "ASIAN", "INDIAN AMERICAN or ALASKA NATIVE",
    "NATIVE HAWAIIAN or PACIFIC ISLANDER", "OTHER", "TWO or MORE RACES", "UNDESIGNATED",
})
KNOWN_ETHNICITIES = frozenset({"HISPANIC or LATINO", "NOT HISPANIC or NOT LATINO", "UNDESIGNATED"})
GENDERS = {"F": "FEMALE", "M": "MALE", "U": "UNDESIGNATED"}
NOT_RECORDED = "NOT RECORDED"

# NCSBE's own age bands (absentee_demo_stats files).
AGE_BANDS = ((25, "18-25"), (40, "26-40"), (65, "41-65"), (200, "Over 65"))
UNKNOWN_AGE = "Unknown"
# A few bad ages are data-entry errors. Many mean the column changed meaning.
MAX_UNKNOWN_AGE_SHARE = 0.005

EARLIEST_RETURN_DAYS = 70          # no return date earlier than this many days before the election
CROSSCHECK_TOLERANCE = 0.005       # ZIP vs. counts file
CROSSCHECK_MAX_GAP = dt.timedelta(hours=6)


def age_band(raw: str) -> str:
    """NCSBE age band, or UNKNOWN_AGE for a blank or impossible age (e.g. a typo like 269)."""
    raw = raw.strip()
    if not raw.isdigit():
        return UNKNOWN_AGE
    age = int(raw)
    if age < 17 or age > 120:
        return UNKNOWN_AGE
    for upper, label in AGE_BANDS:
        if age <= upper:
            return label
    raise AssertionError("unreachable")


def clean(raw: str) -> str:
    return raw.strip().lstrip("'").strip()


def parse_mdy(raw: str) -> dt.date:
    try:
        return dt.datetime.strptime(raw.strip(), "%m/%d/%Y").date()
    except ValueError as exc:
        raise SourceError(f"bad date {raw!r}") from exc


@dataclass
class Tally:
    """Accepted ballots keyed by (county, method, party, age, race, ethnicity, gender, return_date)."""

    counts: Counter = field(default_factory=Counter)
    records: int = 0
    accepted: int = 0
    statuses: Counter = field(default_factory=Counter)
    warnings: list[str] = field(default_factory=list)
    unknown_values: dict[str, set] = field(default_factory=dict)

    def note_unknown(self, kind: str, value: str) -> None:
        self.unknown_values.setdefault(kind, set()).add(value)


DIMS = {"party": 2, "age": 3, "race": 4, "ethnicity": 5, "gender": 6, "return_date": 7}


def iter_records(zip_path: Path, *, election: dt.date) -> Iterator[list[str]]:
    """Yield raw rows from the record-level ZIP after checking its structure."""
    try:
        zf = zipfile.ZipFile(zip_path)
    except zipfile.BadZipFile as exc:
        raise SourceError(f"not a valid ZIP: {exc}") from exc
    with zf:
        members = [m for m in zf.infolist() if not m.is_dir()]
        if len(members) != 1 or not members[0].filename.lower().endswith(".csv"):
            raise SourceError(f"expected exactly one CSV in the ZIP, found {[m.filename for m in members]}")
        election_str = election.strftime("%m/%d/%Y")
        with zf.open(members[0]) as raw:
            text = io.TextIOWrapper(raw, encoding="latin-1", newline="")
            reader = csv.reader(text)
            header = tuple(h.strip() for h in next(reader, ()))
            if header != RECORD_HEADER:
                raise SourceError(f"record file header changed: {list(header)}")
            width = len(RECORD_HEADER)
            for line_no, row in enumerate(reader, start=2):
                if len(row) != width:
                    raise SourceError(f"line {line_no} has {len(row)} fields, expected {width}")
                if row[COL["election_dt"]].strip() != election_str:
                    raise SourceError(f"line {line_no} is for election {row[COL['election_dt']]!r}")
                yield row


def tally_records(rows: Iterator[list[str]], *, election: dt.date, cutoff: dt.date | None) -> Tally:
    """Count accepted ballots. ``cutoff`` excludes ballots returned after that date."""
    t = Tally()
    earliest = election - dt.timedelta(days=EARLIEST_RETURN_DAYS)
    latest = cutoff + dt.timedelta(days=1) if cutoff else election + dt.timedelta(days=14)
    excluded_after_cutoff = 0
    unknown_age = 0
    for row in rows:
        t.records += 1
        county = row[COL["county_desc"]].strip().upper()
        if county not in COUNTY_SET:
            raise SourceError(f"unknown county {county!r}")
        req_type = row[COL["ballot_req_type"]].strip()
        method = RECORD_METHODS.get(req_type)
        if method is None:
            raise SourceError(f"unknown ballot_req_type {req_type!r}")
        status = row[COL["ballot_rtn_status"]].strip()
        t.statuses[status] += 1
        if status not in ACCEPTED:
            if status not in KNOWN_OTHER_STATUSES:
                t.note_unknown("ballot_rtn_status", status)
            continue

        returned = parse_mdy(row[COL["ballot_rtn_dt"]])
        if not (earliest <= returned <= latest):
            raise SourceError(f"return date {returned} outside {earliest}..{latest}")
        if cutoff and returned > cutoff:
            excluded_after_cutoff += 1
            continue

        # A blank field is kept as its own category, never folded into another.
        # Stray spreadsheet apostrophes (e.g. "'UNDESIGNATED" in 2022) are stripped.
        party = clean(row[COL["voter_party_code"]]) or NOT_RECORDED
        race = clean(row[COL["race"]]) or NOT_RECORDED
        ethnicity = clean(row[COL["ethnicity"]]) or NOT_RECORDED
        gender_code = row[COL["gender"]].strip()
        gender = GENDERS.get(gender_code) or gender_code or NOT_RECORDED
        if party not in KNOWN_PARTIES and party != NOT_RECORDED:
            t.note_unknown("party", party)
        if race not in KNOWN_RACES and race != NOT_RECORDED:
            t.note_unknown("race", race)
        if ethnicity not in KNOWN_ETHNICITIES and ethnicity != NOT_RECORDED:
            t.note_unknown("ethnicity", ethnicity)
        if gender_code and gender_code not in GENDERS:
            t.note_unknown("gender", gender_code)

        band = age_band(row[COL["age"]])
        if band == UNKNOWN_AGE:
            unknown_age += 1
        t.accepted += 1
        t.counts[(county, method, party, band, race, ethnicity, gender, returned.isoformat())] += 1

    if t.accepted and unknown_age / t.accepted > MAX_UNKNOWN_AGE_SHARE:
        raise SourceError(f"{unknown_age:,} of {t.accepted:,} accepted ballots have a blank or impossible age")
    if unknown_age:
        t.warnings.append(f"{unknown_age:,} ballot(s) with a blank or impossible age are counted as Unknown")
    if excluded_after_cutoff:
        t.warnings.append(
            f"{excluded_after_cutoff:,} ballot(s) returned on the publication day are left for the next edition"
        )
    for kind, values in sorted(t.unknown_values.items()):
        t.warnings.append(f"new {kind} value(s) seen: {sorted(values)}")
    t.accepted_including_cutoff = t.accepted + excluded_after_cutoff  # type: ignore[attr-defined]
    return t


def tally_to_rows(t: Tally, *, as_of_date: str, county_groups=("party", "age", "race", "ethnicity", "gender",
                                                                 "return_date")) -> list[Row]:
    rows = marginalize(t.counts, state=STATE, as_of_date=as_of_date, dims=DIMS, county_index=0,
                       method_index=1, include_county_groups=county_groups)
    have = {(r.county, r.method) for r in rows if r.group_type == "total"}
    for county in COUNTIES + (STATEWIDE,):
        for method in ("mail", "early_in_person", "all"):
            if (county, method) not in have:
                rows.append(Row(STATE, county, as_of_date, method, "total", TOTAL_VALUE, 0))
    return rows


def parse_state_counts(text: str) -> dict[str, int]:
    """Sum NCSBE's statewide counts file by method, with strict structure checks."""
    reader = csv.reader(io.StringIO(text))
    header = tuple(h.strip() for h in next(reader, ()))
    if header != STATE_COUNTS_HEADER:
        raise SourceError(f"state counts header changed: {list(header)}")
    by_method: Counter = Counter()
    combos = set()
    distinct = [set() for _ in range(len(header) - 1)]
    for line_no, row in enumerate(reader, start=2):
        if len(row) != len(header):
            raise SourceError(f"state counts line {line_no} has {len(row)} fields")
        req_type = row[0].strip()
        method = COUNTS_METHODS.get(req_type)
        if method is None:
            raise SourceError(f"unknown abs_request_type {req_type!r} in state counts")
        if not row[-1].strip().isdigit():
            raise SourceError(f"non-numeric count {row[-1]!r} in state counts")
        key = tuple(v.strip() for v in row[:-1])
        if key in combos:
            raise SourceError(f"duplicate combination {key} in state counts")
        combos.add(key)
        for i, v in enumerate(key):
            distinct[i].add(v)
        by_method[method] += int(row[-1])
    # The file is a full cross-tab: its row count must equal the product of
    # distinct values per column. A partial or truncated file fails here.
    expected_rows = 1
    for d in distinct:
        expected_rows *= len(d)
    if len(combos) != expected_rows or expected_rows == 0:
        raise SourceError(f"state counts has {len(combos)} rows, expected full cross-tab of {expected_rows}")
    return dict(by_method)


@dataclass
class ScrapeResult:
    rows: list[Row]
    as_of_date: str
    meta: dict


def eastern_date(ts: dt.datetime) -> dt.date:
    return ts.astimezone(EASTERN).date()


def source_version() -> str:
    """Cheap fingerprint of the current publication (ETag + Last-Modified)."""
    head = http.head(RECORDS_URL)
    return f"{head.etag}|{head.last_modified.isoformat()}"


def scrape() -> ScrapeResult:
    with tempfile.TemporaryDirectory() as tmp:
        zip_path = Path(tmp) / "records.zip"
        records_meta = http.download(RECORDS_URL, zip_path)
        as_of = eastern_date(records_meta.last_modified)
        cutoff = as_of - dt.timedelta(days=1)
        tally = tally_records(iter_records(zip_path, election=ELECTION_DATE), election=ELECTION_DATE,
                              cutoff=cutoff)

    if tally.records == 0:
        raise SourceError("record file has no rows")
    if tally.accepted == 0:
        raise SourceError("record file has no accepted ballots")

    counts_text, counts_meta = http.get_text(STATE_COUNTS_URL, encoding="latin-1")
    official = parse_state_counts(counts_text)
    official_total = sum(official.values())
    ours = tally.accepted_including_cutoff  # type: ignore[attr-defined]
    gap = abs(counts_meta.last_modified - records_meta.last_modified)
    crosscheck: dict = {"official_total": official_total, "record_file_total": ours}
    if gap <= CROSSCHECK_MAX_GAP:
        allowed = max(1, round(official_total * CROSSCHECK_TOLERANCE))
        if abs(official_total - ours) > allowed:
            raise SourceError(
                f"record file has {ours:,} accepted ballots but NCSBE's counts file says {official_total:,}"
            )
        crosscheck["status"] = "matched" if official_total == ours else "within tolerance"
    else:
        crosscheck["status"] = "skipped"
        tally.warnings.append(
            f"cross-check skipped: counts file and record file were published {gap} apart"
        )

    as_of_iso = as_of.isoformat()
    rows = tally_to_rows(tally, as_of_date=as_of_iso)
    meta = {
        "state": STATE,
        "name": NAME,
        "as_of_date": as_of_iso,
        "complete_through": cutoff.isoformat(),
        "election_date": ELECTION_DATE.isoformat(),
        "source_last_modified": records_meta.last_modified.isoformat(),
        "source_version": f"{records_meta.etag}|{records_meta.last_modified.isoformat()}",
        "records_read": tally.records,
        "accepted_ballots": tally.accepted,
        "crosscheck": crosscheck,
        "warnings": tally.warnings,
        "sources": [
            {"label": "Absentee and early-voting records (ZIP)", "url": RECORDS_URL,
             "last_modified": records_meta.last_modified.isoformat()},
            {"label": "Statewide absentee counts (CSV, cross-check)", "url": STATE_COUNTS_URL,
             "last_modified": counts_meta.last_modified.isoformat()},
            {"label": "NCSBE absentee data page", "url": SOURCE_PAGE},
        ],
    }
    return ScrapeResult(rows=rows, as_of_date=as_of_iso, meta=meta)
