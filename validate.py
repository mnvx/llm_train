"""
Validates data files before git commit.
Exits with code 1 (fails the CI step) if anything looks wrong.
"""

import json
import sys
import logging
from pathlib import Path
from datetime import date, timedelta

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)-8s %(message)s")

DATA_DIR = Path(__file__).parent / "data"

DAILY_REQUIRED_FIELDS = {"date", "symbol", "o", "h", "l", "c"}
MINUTE_REQUIRED_FIELDS = {"t", "o", "h", "l", "c"}

errors = []


def check(condition: bool, message: str) -> None:
    """Log an error if the condition is false. The message should describe the failure."""
    if not condition:
        errors.append(message)
        log.error("FAIL  %s", message)


def validate_daily(path: Path) -> None:
    rel_path = path.relative_to(DATA_DIR)
    rows = []
    with open(path) as f:
        for i, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as e:
                errors.append(f"{rel_path}:{i} invalid JSON: {e}")
                continue
            rows.append(row)

            # Required fields
            missing = DAILY_REQUIRED_FIELDS - set(row.keys())
            check(not missing, f"{rel_path}:{i} missing fields: {missing}")

            # OHLC sanity
            try:
                check(row["l"] <= row["o"] <= row["h"], f"{rel_path}:{i} OHLC out of order (l={row['l']} o={row['o']} h={row['h']})")
                check(row["l"] <= row["c"] <= row["h"], f"{rel_path}:{i} close outside high/low range")
                check(row["o"] > 0, f"{rel_path}:{i} open price is zero or negative")
            except (KeyError, TypeError):
                pass

    # No duplicate dates
    dates = [r.get("date") for r in rows if "date" in r]
    check(len(dates) == len(set(dates)), f"{rel_path} contains duplicate dates: {[d for d in dates if dates.count(d) > 1]}")

    # Sorted chronologically
    check(dates == sorted(dates), f"{rel_path} rows are not in chronological order")

    log.info("  %s: %d records", rel_path, len(rows))


def validate_minute_file(path: Path) -> None:
    rel_path = path.relative_to(DATA_DIR)
    rows = []
    with open(path) as f:
        for i, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as e:
                errors.append(f"{rel_path}:{i} invalid JSON: {e}")
                continue
            rows.append(row)

            missing = MINUTE_REQUIRED_FIELDS - set(row.keys())
            check(not missing, f"{rel_path}:{i} missing fields: {missing}")

    check(len(rows) > 0, f"{rel_path} is empty")
    # Reasonable bar count (stocks ~390, crypto up to 1440, allow ±20%)
    check(50 <= len(rows) <= 1500, f"{rel_path} unexpected bar count: {len(rows)}")

    if rows:
        log.info("  %s: %d records", rel_path, len(rows))


def main() -> None:
    if not DATA_DIR.exists():
        log.warning("data/ directory does not exist yet — skipping validation.")
        sys.exit(0)

    for symbol_dir in sorted(DATA_DIR.iterdir()):
        if not symbol_dir.is_dir():
            continue

        log.info("── Validating %s ──", symbol_dir.name)

        daily_path = symbol_dir / "daily.jsonl"
        if daily_path.exists():
            validate_daily(daily_path)
        else:
            log.warning("%s missing daily.jsonl", symbol_dir.name)

        minutes_dir = symbol_dir / "minutes"
        if minutes_dir.exists():
            for minute_file in sorted(minutes_dir.glob("*.jsonl")):
                validate_minute_file(minute_file)

    if errors:
        log.error("\n%d validation error(s) found:", len(errors))
        for e in errors:
            log.error("  • %s", e)
        sys.exit(1)
    else:
        log.info("\nAll validations passed.")
        sys.exit(0)


if __name__ == "__main__":
    main()
