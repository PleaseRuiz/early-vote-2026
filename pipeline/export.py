"""Turn snapshots, baselines and status into the JSON the page reads.

Everything here is arithmetic on direct counts: sums, differences between
consecutive days, and shares. Nothing is estimated, smoothed or filled in.
Comparisons with 2022 line up the same number of days before Election Day.

Output: site/public/data/summary.json
"""

from __future__ import annotations

import datetime as dt
import json
import sys
from collections import defaultdict
from pathlib import Path

from pipeline.run import DATA, EASTERN, ROOT, SNAPSHOTS, STATUS_PATH, STATES
from pipeline.schema import STATEWIDE, read_rows

OUT = ROOT / "site" / "public" / "data" / "summary.json"
REPO_URL = "https://github.com/PleaseRuiz/early-vote-2026"

BASELINES = {
    "NC": {"year": 2022, "election": dt.date(2022, 11, 8), "path": DATA / "baselines" / "nc_2022.csv",
           "source": "https://s3.amazonaws.com/dl.ncsbe.gov/ENRS/2022_11_08/absentee_20221108.zip"},
}
ELECTIONS = {"NC": dt.date(2026, 11, 3)}
# In-person early voting opens (first day ballots are cast at one-stop sites).
EARLY_VOTING_OPENS = {"NC": {2026: dt.date(2026, 10, 15), 2022: dt.date(2022, 10, 20)}}
EARLY_VOTING_CLOSES = {"NC": {2026: dt.date(2026, 10, 31), 2022: dt.date(2022, 11, 5)}}

METHODS = ("all", "mail", "early_in_person")

# Display groups for the composition chart. Raw categories are kept in "detail".
DISPLAY_GROUPS = {
    "party": ("Party registration", [
        ("DEM", "Democrats", {"DEM"}),
        ("REP", "Republicans", {"REP"}),
        ("UNA", "Unaffiliated", {"UNA"}),
        ("OTH", "Other parties", None),
    ]),
    "age": ("Age", [
        ("18-25", "18 to 25", {"18-25"}),
        ("26-40", "26 to 40", {"26-40"}),
        ("41-65", "41 to 65", {"41-65"}),
        ("Over 65", "Over 65", {"Over 65"}),
        ("Unknown", "Age unknown", None),
    ]),
    "race": ("Race", [
        ("WHITE", "White", {"WHITE"}),
        ("BLACK", "Black", {"BLACK or AFRICAN AMERICAN"}),
        ("OTHER", "Other races", {"ASIAN", "INDIAN AMERICAN or ALASKA NATIVE", "NATIVE HAWAIIAN or PACIFIC ISLANDER",
                                  "OTHER", "TWO or MORE RACES"}),
        ("UND", "Not designated", None),
    ]),
    "ethnicity": ("Ethnicity", [
        ("HISP", "Hispanic or Latino", {"HISPANIC or LATINO"}),
        ("NOT", "Not Hispanic or Latino", {"NOT HISPANIC or NOT LATINO"}),
        ("UND", "Not designated", None),
    ]),
    "gender": ("Sex", [
        ("FEMALE", "Female", {"FEMALE"}),
        ("MALE", "Male", {"MALE"}),
        ("UND", "Not designated", None),
    ]),
}

RAW_LABELS = {
    "DEM": "Democrat", "REP": "Republican", "UNA": "Unaffiliated", "LIB": "Libertarian", "GRE": "Green",
    "CST": "Constitution", "JFA": "Justice for All", "NLB": "No Labels", "WTP": "We the People",
}


def title_county(name: str) -> str:
    special = {"MCDOWELL": "McDowell", "NEW HANOVER": "New Hanover"}
    return special.get(name, name.title())


def index(rows):
    """{as_of: {(county, method, group_type, value): ballots}}"""
    out: dict[str, dict[tuple, int]] = defaultdict(dict)
    for r in rows:
        out[r.as_of_date][(r.county, r.method, r.group_type, r.group_value)] = r.ballots
    return out


def total(ix: dict, county=STATEWIDE, method="all") -> int:
    return ix.get((county, method, "total", "all"), 0)


def groups(ix: dict, gtype: str, method="all", county=STATEWIDE) -> dict[str, int]:
    return {k[3]: n for k, n in ix.items() if k[0] == county and k[1] == method and k[2] == gtype}


def display_composition(gtype: str, now: dict[str, int], base: dict[str, int]) -> dict:
    label, spec = DISPLAY_GROUPS[gtype]
    explicit = set().union(*(s for _, _, s in spec if s))

    def bucket(values: dict[str, int], members):
        if members is None:
            return sum(n for v, n in values.items() if v not in explicit)
        return sum(values.get(v, 0) for v in members)

    now_total, base_total = sum(now.values()), sum(base.values())
    segments = []
    for key, seg_label, members in spec:
        n, b = bucket(now, members), bucket(base, members)
        if members is None and n == 0 and b == 0:
            continue
        segments.append({
            "key": key, "label": seg_label, "now": n, "base": b,
            "now_share": n / now_total if now_total else None,
            "base_share": b / base_total if base_total else None,
        })
    detail = sorted(
        ({"value": RAW_LABELS.get(v, v.title() if v.isupper() else v), "now": n, "base": base.get(v, 0)}
         for v, n in now.items()),
        key=lambda d: -d["now"])
    for v, b in base.items():
        if v not in now:
            detail.append({"value": RAW_LABELS.get(v, v.title() if v.isupper() else v), "now": 0, "base": b})
    return {"type": gtype, "label": label, "now_total": now_total, "base_total": base_total,
            "segments": segments, "detail": detail}


def build_state(code: str, status_entry: dict, today: dt.date) -> dict | None:
    snaps = sorted((SNAPSHOTS / code.lower()).glob("????-??-??.csv"))
    if not snaps:
        return None
    latest_path = snaps[-1]
    latest_rows = read_rows(latest_path)
    as_of = dt.date.fromisoformat(latest_path.stem)
    through = as_of - dt.timedelta(days=1)
    election = ELECTIONS[code]
    ix = index(latest_rows)[as_of.isoformat()]

    base_cfg = BASELINES[code]
    base_ix_all = index(read_rows(base_cfg["path"]))
    base_election = base_cfg["election"]
    days_before = (election - through).days  # days before Election Day of the last counted day

    def base_for_days_before(d: int) -> dict:
        # Baseline as_of is the morning after the counted day.
        b_as_of = base_election - dt.timedelta(days=d) + dt.timedelta(days=1)
        return base_ix_all.get(b_as_of.isoformat(), {})

    base_ix = base_for_days_before(days_before)
    base_through = base_election - dt.timedelta(days=days_before)
    base_final_ix = base_ix_all[base_election.isoformat()]  # morning of Election Day 2022

    # ---- Cumulative curves by the day ballots were returned ----
    curve_now = []
    by_day_now = defaultdict(lambda: dict.fromkeys(METHODS, 0))
    for (county, method, gtype, value), n in ix.items():
        if county == STATEWIDE and gtype == "return_date":
            by_day_now[value][method] += n
    running = dict.fromkeys(METHODS, 0)
    daily_now = []
    for day in sorted(by_day_now):
        d = dt.date.fromisoformat(day)
        counts = by_day_now[day]
        for m in METHODS:
            running[m] += counts[m]
        db = (election - d).days
        curve_now.append({"date": day, "days_before": db, **{m: running[m] for m in METHODS}})
        daily_now.append({"date": day, "days_before": db, **{m: counts[m] for m in METHODS}})

    curve_base, daily_base = [], []
    prev = None
    for b_as_of in sorted(base_ix_all):
        bx = base_ix_all[b_as_of]
        d = dt.date.fromisoformat(b_as_of) - dt.timedelta(days=1)
        point = {"date": d.isoformat(), "days_before": (base_election - d).days,
                 **{m: total(bx, method=m) for m in METHODS}}
        curve_base.append(point)
        if prev is not None:
            daily_base.append({"date": point["date"], "days_before": point["days_before"],
                               **{m: point[m] - prev[m] for m in METHODS}})
        prev = point

    # ---- Counties ----
    from pipeline.sources import nc as nc_source  # only NC in Phase 1
    counties = []
    for key in nc_source.COUNTIES:
        row = {"key": key, "name": title_county(key)}
        for m in METHODS:
            row[m] = total(ix, key, m)
            row[f"base_{m}"] = total(base_ix, key, m)
        row["base_final"] = total(base_final_ix, key, "all")
        counties.append(row)

    # ---- Composition ----
    composition = [display_composition(g, groups(ix, g), groups(base_ix, g)) for g in DISPLAY_GROUPS]

    # ---- Editions (one per daily snapshot) ----
    editions = []
    for p in snaps:
        rows = read_rows(p)
        editions.append({"as_of": p.stem, "ballots": total(index(rows)[p.stem]),
                         "csv": f"{REPO_URL}/blob/main/{p.relative_to(ROOT).as_posix()}"})

    good = status_entry.get("good") or {}
    stale_reasons = []
    if status_entry.get("ok") is False:
        stale_reasons.append(f"The last attempt to read the source failed: {status_entry.get('last_error')}")
    if (today - as_of).days >= 2:
        stale_reasons.append(f"The source has not published new data since {as_of:%B} {as_of.day}.")

    headline = {m: total(ix, method=m) for m in METHODS}
    base_headline = {m: total(base_ix, method=m) for m in METHODS}

    return {
        "code": code,
        "name": status_entry.get("name", code),
        "election_date": election.isoformat(),
        "as_of": as_of.isoformat(),
        "through": through.isoformat(),
        "days_before": days_before,
        "edition": len(snaps),
        "stale": bool(stale_reasons),
        "stale_reasons": stale_reasons,
        "last_attempt_at": status_entry.get("last_attempt_at"),
        "last_success_at": status_entry.get("last_success_at"),
        "source_last_modified": good.get("source_last_modified"),
        "crosscheck": good.get("crosscheck"),
        "warnings": good.get("warnings", []),
        "sources": good.get("sources", []),
        "snapshot_csv": f"{REPO_URL}/blob/main/{latest_path.relative_to(ROOT).as_posix()}",
        "now": headline,
        "base": {
            "year": base_cfg["year"], "election_date": base_election.isoformat(),
            "through": base_through.isoformat(), **base_headline,
            "final_before_election_day": total(base_final_ix),
            "source": base_cfg["source"],
            "csv": f"{REPO_URL}/blob/main/{base_cfg['path'].relative_to(ROOT).as_posix()}",
        },
        "early_voting": {
            "now": {"opens": EARLY_VOTING_OPENS[code][2026].isoformat(),
                    "closes": EARLY_VOTING_CLOSES[code][2026].isoformat()},
            "base": {"opens": EARLY_VOTING_OPENS[code][2022].isoformat(),
                     "closes": EARLY_VOTING_CLOSES[code][2022].isoformat()},
        },
        "curve": {"now": curve_now, "base": curve_base},
        "daily": {"now": daily_now, "base": daily_base},
        "counties": counties,
        "composition": composition,
        "editions": editions,
    }


def main() -> int:
    status = json.loads(STATUS_PATH.read_text(encoding="utf-8")) if STATUS_PATH.exists() else {"states": {}}
    today = dt.datetime.now(EASTERN).date()
    states = []
    for code in STATES:
        s = build_state(code, status.get("states", {}).get(code, {}), today)
        if s:
            states.append(s)
    out = {
        "generated_at": dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat(),
        "repo": REPO_URL,
        "states": states,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(out, separators=(",", ":")), encoding="utf-8")
    print(f"Wrote {OUT.relative_to(ROOT)} ({OUT.stat().st_size / 1024:.0f} KB, {len(states)} state(s))")
    return 0


if __name__ == "__main__":
    sys.exit(main())
