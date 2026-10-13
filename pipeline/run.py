"""Run every state scraper once. Used by the daily GitHub Actions job.

For each state:
  1. Ask the source for a cheap fingerprint. If it hasn't changed since the
     last good run, record "unchanged" and move on.
  2. Otherwise scrape, validate, and compare with the last good snapshot.
  3. On success, write data/snapshots/<state>/<as_of_date>.csv.
  4. On any failure, write nothing for that state. The last good snapshot
     stays, and data/status.json records the error so the page can label the
     state as stale.

Exits with status 1 if any state failed, after writing status.json, so the
workflow can still publish the other states and then go red.
"""

from __future__ import annotations

import argparse
import datetime as dt
import importlib
import json
import os
import sys
import traceback
from pathlib import Path
from zoneinfo import ZoneInfo

from pipeline.schema import STATEWIDE, SourceError, read_rows, validate_rows, write_rows

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
SNAPSHOTS = DATA / "snapshots"
STATUS_PATH = DATA / "status.json"
EASTERN = ZoneInfo("America/New_York")

# State code → module. Add Phase 2 states here.
STATES = {"NC": "pipeline.sources.nc"}

# The job keeps running through the morning after Election Day, then stops.
LAST_RUN_DATE = dt.date(2026, 11, 4)
# A statewide total may dip slightly as ballots are cured or rejected. A bigger
# drop means the source is broken, so the run fails.
MAX_DROP = 0.01
REQUIRED_BREAKDOWNS = ("party", "age", "race", "ethnicity", "gender", "return_date")


def now_utc() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def load_status() -> dict:
    if STATUS_PATH.exists():
        return json.loads(STATUS_PATH.read_text(encoding="utf-8"))
    return {"states": {}}


def save_status(status: dict) -> None:
    status["generated_at"] = now_utc()
    STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATUS_PATH.write_text(json.dumps(status, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def snapshot_dir(code: str) -> Path:
    return SNAPSHOTS / code.lower()


def latest_snapshot(code: str) -> Path | None:
    files = sorted(snapshot_dir(code).glob("????-??-??.csv"))
    return files[-1] if files else None


def statewide_total(rows) -> int:
    for r in rows:
        if r.county == STATEWIDE and r.method == "all" and r.group_type == "total":
            return r.ballots
    raise SourceError("no statewide total row")


def annotate(level: str, title: str, message: str) -> None:
    if os.environ.get("GITHUB_ACTIONS"):
        print(f"::{level} title={title}::{message}")
    else:
        print(f"[{level}] {title}: {message}")


def run_state(code: str, module_name: str, status: dict) -> bool:
    entry = status.setdefault("states", {}).setdefault(code, {})
    entry["last_attempt_at"] = now_utc()
    try:
        module = importlib.import_module(module_name)
        entry["name"] = module.NAME
        previous = latest_snapshot(code)
        good = entry.get("good") or {}

        version = module.source_version()
        if previous and version == good.get("source_version"):
            entry.update(ok=True, last_result="unchanged", last_error=None)
            print(f"{code}: source unchanged since {good.get('source_last_modified')}")
            return True

        result = module.scrape()
        rows = validate_rows(result.rows, require_breakdowns=REQUIRED_BREAKDOWNS)
        new_total = statewide_total(rows)

        if previous:
            prev_as_of = previous.stem
            if result.as_of_date < prev_as_of:
                raise SourceError(f"source data is dated {result.as_of_date}, older than the last snapshot ({prev_as_of})")
            prev_total = statewide_total(read_rows(previous))
            if new_total < prev_total * (1 - MAX_DROP):
                raise SourceError(
                    f"statewide total fell from {prev_total:,} to {new_total:,}, more than {MAX_DROP:.0%}"
                )

        path = snapshot_dir(code) / f"{result.as_of_date}.csv"
        write_rows(path, rows)
        meta = dict(result.meta)
        meta["snapshot"] = str(path.relative_to(ROOT))
        meta["fetched_at"] = entry["last_attempt_at"]
        meta["statewide_total"] = new_total
        entry.update(ok=True, last_result="updated", last_error=None, last_success_at=entry["last_attempt_at"],
                     good=meta)
        for w in meta.get("warnings", []):
            annotate("warning", f"{code} source", w)
        print(f"{code}: wrote {path.relative_to(ROOT)} ({len(rows):,} rows, {new_total:,} ballots)")
        return True
    except Exception as exc:  # noqa: BLE001 - any failure must keep the last good snapshot
        message = str(exc) if isinstance(exc, SourceError) else f"{type(exc).__name__}: {exc}"
        entry.update(ok=False, last_result="failed", last_error=message, last_failed_at=entry["last_attempt_at"])
        annotate("error", f"{code} source failed", message)
        traceback.print_exc()
        return False


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--state", action="append", help="run only these state codes")
    parser.add_argument("--force", action="store_true", help="run even after the last scheduled date")
    args = parser.parse_args(argv)

    today = dt.datetime.now(EASTERN).date()
    if today > LAST_RUN_DATE and not args.force:
        print(f"Today is {today}; scraping stopped after {LAST_RUN_DATE}. Nothing to do.")
        return 0

    status = load_status()
    selected = {k: v for k, v in STATES.items() if not args.state or k in args.state}
    results = {code: run_state(code, mod, status) for code, mod in selected.items()}
    save_status(status)

    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as f:
            f.write("| State | Result | Data as of | Ballots | Error |\n|---|---|---|---|---|\n")
            for code in selected:
                e = status["states"][code]
                g = e.get("good") or {}
                f.write(f"| {code} | {e.get('last_result')} | {g.get('as_of_date', '—')} | "
                        f"{g.get('statewide_total', 0):,} | {e.get('last_error') or ''} |\n")

    failed = [code for code, ok in results.items() if not ok]
    if failed:
        print(f"Failed: {', '.join(failed)}. Their last good snapshots were kept.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
