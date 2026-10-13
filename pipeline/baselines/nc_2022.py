"""Build the 2022 North Carolina baseline from NCSBE's final 2022 record file.

NCSBE did not keep its daily 2022 files, only the final one (Nov. 19, 2022).
Every ballot in it carries the date it was returned, so for each morning
before the 2022 election we count the ballots that were *ultimately accepted*
and had been returned by the end of the previous day. These are direct counts,
not estimates.

Output rows use the normalized schema, with as_of_date = that 2022 morning:
  * county totals by method (for the county ledger), and
  * statewide totals and demographic breakdowns by method.

Run once:  python -m pipeline.baselines.nc_2022
"""

from __future__ import annotations

import datetime as dt
import sys
import tempfile
from collections import defaultdict
from pathlib import Path

from pipeline import http
from pipeline.schema import STATEWIDE, TOTAL_VALUE, Row, SourceError, validate_rows, write_rows
from pipeline.sources import nc

ELECTION_2022 = dt.date(2022, 11, 8)
RECORDS_URL_2022 = "https://s3.amazonaws.com/dl.ncsbe.gov/ENRS/2022_11_08/absentee_20221108.zip"
# NCSBE's published 2022 total of accepted absentee and early ballots.
EXPECTED_FINAL_ACCEPTED = 2_198_550
DAYS_OUT = range(66, -1, -1)
OUT = Path(__file__).resolve().parents[2] / "data" / "baselines" / "nc_2022.csv"
GROUPS = ("party", "age", "race", "ethnicity", "gender")


def build(zip_path: Path) -> tuple[list[Row], dict]:
    tally = nc.tally_records(nc.iter_records(zip_path, election=ELECTION_2022), election=ELECTION_2022,
                             cutoff=None)
    if tally.accepted != EXPECTED_FINAL_ACCEPTED:
        raise SourceError(f"2022 file has {tally.accepted:,} accepted ballots, expected {EXPECTED_FINAL_ACCEPTED:,}")

    # by_day[date][(county, method, group_type, value)] = ballots returned that day
    by_day: dict[str, dict[tuple, int]] = defaultdict(lambda: defaultdict(int))
    for key, n in tally.counts.items():
        county, method, *dims, returned = key
        day = by_day[returned]
        for c in (county, STATEWIDE):
            for m in (method, "all"):
                day[(c, m, "total", TOTAL_VALUE)] += n
                if c == STATEWIDE:
                    for gtype, value in zip(GROUPS, dims):
                        day[(c, m, gtype, value)] += n

    rows: list[Row] = []
    running: dict[tuple, int] = defaultdict(int)
    days = sorted(by_day)
    i = 0
    for days_out in DAYS_OUT:
        as_of = ELECTION_2022 - dt.timedelta(days=days_out)
        through = (as_of - dt.timedelta(days=1)).isoformat()
        while i < len(days) and days[i] <= through:
            for k, n in by_day[days[i]].items():
                running[k] += n
            i += 1
        snapshot = dict(running)
        for county in nc.COUNTIES + (STATEWIDE,):
            for method in ("mail", "early_in_person", "all"):
                snapshot.setdefault((county, method, "total", TOTAL_VALUE), 0)
        rows.extend(Row("NC", c, as_of.isoformat(), m, g, v, n) for (c, m, g, v), n in snapshot.items())

    validate_rows(rows)
    info = {"accepted_final": tally.accepted, "records": tally.records, "warnings": tally.warnings}
    return rows, info


def main() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "absentee_20221108.zip"
        print(f"Downloading {RECORDS_URL_2022} …", flush=True)
        http.download(RECORDS_URL_2022, path)
        rows, info = build(path)
    write_rows(OUT, rows)
    final = next(r.ballots for r in rows if r.as_of_date == ELECTION_2022.isoformat() and r.county == STATEWIDE
                 and r.method == "all" and r.group_type == "total")
    print(f"Read {info['records']:,} records; {info['accepted_final']:,} accepted (matches NCSBE).")
    print(f"Accepted and returned before Election Day 2022: {final:,}")
    for w in info["warnings"]:
        print("warning:", w)
    print(f"Wrote {OUT} ({len(rows):,} rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
