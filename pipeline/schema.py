"""The normalized schema every state scraper writes.

One row is one count of accepted ballots:

    state, county, as_of_date, method, group_type, group_value, ballots

* ``county`` is a county name, or ``ALL`` for statewide rows.
* ``as_of_date`` is the morning the source published the data (Eastern time).
  Rows count ballots returned through the day *before* ``as_of_date``, so a
  partially entered publication day is never mixed in.
* ``method`` is ``mail``, ``early_in_person`` or ``all``.
* ``group_type`` is one of GROUP_TYPES. ``total`` rows always use the value
  ``all``. ``return_date`` values are ISO dates.

Every breakdown must add up to its total. ``validate_rows`` enforces that, so a
bug in aggregation fails the run instead of reaching the page.
"""

from __future__ import annotations

import csv
import datetime as dt
import io
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

FIELDS = ("state", "county", "as_of_date", "method", "group_type", "group_value", "ballots")
METHODS = ("mail", "early_in_person", "all")
GROUP_TYPES = ("total", "party", "age", "race", "ethnicity", "gender", "return_date")
STATEWIDE = "ALL"
TOTAL_VALUE = "all"

_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_STATE = re.compile(r"^[A-Z]{2}$")


class SourceError(Exception):
    """A source failed validation. Nothing is written for that state on this run."""


@dataclass(frozen=True, order=True)
class Row:
    state: str
    county: str
    as_of_date: str
    method: str
    group_type: str
    group_value: str
    ballots: int

    def key(self) -> tuple:
        return (self.state, self.county, self.as_of_date, self.method, self.group_type, self.group_value)


def _is_iso_date(value: str) -> bool:
    if not _ISO_DATE.match(value):
        return False
    try:
        dt.date.fromisoformat(value)
    except ValueError:
        return False
    return True


def validate_rows(rows: Iterable[Row], *, require_breakdowns: Iterable[str] = ()) -> list[Row]:
    """Check every row and the arithmetic between rows. Raises SourceError."""
    rows = list(rows)
    if not rows:
        raise SourceError("no rows produced")

    seen: set[tuple] = set()
    for r in rows:
        if not _STATE.match(r.state):
            raise SourceError(f"bad state code {r.state!r}")
        if not r.county:
            raise SourceError("empty county")
        if not _is_iso_date(r.as_of_date):
            raise SourceError(f"bad as_of_date {r.as_of_date!r}")
        if r.method not in METHODS:
            raise SourceError(f"unknown method {r.method!r}")
        if r.group_type not in GROUP_TYPES:
            raise SourceError(f"unknown group_type {r.group_type!r}")
        if not r.group_value:
            raise SourceError(f"empty group_value for {r.group_type}")
        if r.group_type == "total" and r.group_value != TOTAL_VALUE:
            raise SourceError(f"total rows must use group_value {TOTAL_VALUE!r}")
        if r.group_type == "return_date" and not _is_iso_date(r.group_value):
            raise SourceError(f"bad return_date {r.group_value!r}")
        if not isinstance(r.ballots, int) or isinstance(r.ballots, bool) or r.ballots < 0:
            raise SourceError(f"bad ballot count {r.ballots!r}")
        k = r.key()
        if k in seen:
            raise SourceError(f"duplicate row {k}")
        seen.add(k)

    # Arithmetic: every breakdown sums to its total, and methods sum to "all".
    totals: dict[tuple, int] = {}
    sums: dict[tuple, int] = defaultdict(int)
    present: dict[tuple, set] = defaultdict(set)
    for r in rows:
        base = (r.state, r.county, r.as_of_date, r.method)
        if r.group_type == "total":
            totals[base] = r.ballots
        else:
            sums[base + (r.group_type,)] += r.ballots
            present[base].add(r.group_type)

    for (state, county, as_of, method, gtype), value in sums.items():
        total = totals.get((state, county, as_of, method))
        if total is None:
            raise SourceError(f"{gtype} rows without a total for {county}/{method}/{as_of}")
        if value != total:
            raise SourceError(
                f"{gtype} breakdown sums to {value:,} but total is {total:,} ({county}/{method}/{as_of})"
            )

    for gtype in require_breakdowns:
        for base, total in totals.items():
            if total > 0 and gtype not in present[base]:
                raise SourceError(f"missing {gtype} breakdown for {base}")

    for (state, county, as_of, method), total in totals.items():
        if method != "all":
            continue
        parts = sum(totals.get((state, county, as_of, m), 0) for m in METHODS if m != "all")
        if parts != total:
            raise SourceError(f"methods sum to {parts:,} but all-methods total is {total:,} ({county}/{as_of})")

    return rows


def rows_to_csv(rows: Iterable[Row]) -> str:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(FIELDS)
    for r in sorted(rows):
        w.writerow([r.state, r.county, r.as_of_date, r.method, r.group_type, r.group_value, r.ballots])
    return buf.getvalue()


def write_rows(path: Path, rows: Iterable[Row]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(rows_to_csv(rows), encoding="utf-8")
    tmp.replace(path)


def read_rows(path: Path) -> list[Row]:
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if tuple(header or ()) != FIELDS:
            raise SourceError(f"{path} has header {header}, expected {list(FIELDS)}")
        return [Row(s, c, a, m, gt, gv, int(b)) for s, c, a, m, gt, gv, b in reader]


def marginalize(counts: dict[tuple, int], *, state: str, as_of_date: str, dims: dict[str, int],
                county_index: int, method_index: int, include_county_groups: Iterable[str]) -> list[Row]:
    """Turn a Counter of full-detail tuples into normalized marginal rows.

    ``counts`` maps a tuple (county, method, <dims...>) to a ballot count.
    ``dims`` maps a group_type to its position in that tuple.
    Statewide (``ALL``) and all-method (``all``) rows are derived by summing.
    Group breakdowns are written for statewide rows always, and for county rows
    only for the group types listed in ``include_county_groups``.
    """
    agg: dict[tuple, int] = defaultdict(int)
    county_groups = set(include_county_groups)
    for key, n in counts.items():
        county, method = key[county_index], key[method_index]
        for c in (county, STATEWIDE):
            for m in (method, "all"):
                agg[(c, m, "total", TOTAL_VALUE)] += n
                for gtype, idx in dims.items():
                    if c == STATEWIDE or gtype in county_groups:
                        agg[(c, m, gtype, key[idx])] += n
    return [Row(state, c, as_of_date, m, gt, gv, n) for (c, m, gt, gv), n in agg.items()]
