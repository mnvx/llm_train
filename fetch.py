"""
Market Dataset Fetcher
Pulls daily + minute data from Twelve Data API and appends to JSONL files.
Run this daily after market close (~21:30 UTC).
"""

import json
import os
import time
import logging
from datetime import date, timedelta, datetime
from pathlib import Path
from typing import Optional

import requests

# ── Config ────────────────────────────────────────────────────────────────────

API_KEY   = os.getenv("TWELVEDATA_API_KEY", "demo")
BASE_URL  = "https://api.twelvedata.com"
DATA_DIR  = Path(__file__).parent / "data"

SYMBOLS = {
    "AAPL":    {"exchange": "NASDAQ", "type": "stock"},
    "BTC/USD": {"exchange": "Coinbase", "type": "crypto"},
}

# Twelve Data demo key: 8 requests/minute, some endpoints restricted
REQUEST_DELAY = 0.1  # seconds between requests (safe for demo key)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ── HTTP helper ───────────────────────────────────────────────────────────────

def get(endpoint: str, params: dict) -> Optional[dict]:
    """GET request with error handling. Returns parsed JSON or None."""
    params["apikey"] = API_KEY
    url = f"{BASE_URL}/{endpoint}"
    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        if data.get("status") == "error":
            log.error("API error for %s: %s", endpoint, data.get("message"))
            return None
        return data
    except requests.RequestException as e:
        log.error("Request failed (%s): %s", url, e)
        return None


# ── Indicator helpers ─────────────────────────────────────────────────────────

def fetch_indicator_series(symbol: str, indicator: str, extra_params: dict) -> dict:
    """Fetch an indicator time series, returning {datetime: values_dict}."""
    params = {
        "symbol": symbol,
        "format": "JSON",
    }
    params.update(extra_params)

    data = get(indicator, params)
    time.sleep(REQUEST_DELAY)

    if not data or "values" not in data:
        return {}

    return {row["datetime"]: row for row in data["values"]}


# ── Daily data ────────────────────────────────────────────────────────────────

def fetch_daily_bar(symbol: str, target_date: date) -> Optional[dict]:
    """Fetch OHLCV daily bar for a specific date."""
    target_str = target_date.isoformat()
    end_str = (target_date + timedelta(days=1)).isoformat()

    data = get("time_series", {
        "symbol":     symbol,
        "interval":   "1day",
        "outputsize": 10,
        "start_date": target_str,
        "end_date":   end_str,
        "format":     "JSON",
    })
    time.sleep(REQUEST_DELAY)

    if not data or "values" not in data:
        return None

    for row in data["values"]:
        if row["datetime"] == target_str:
            return row

    log.warning("No daily bar found for %s on %s", symbol, target_str)
    return None


def build_daily_record(symbol: str, bar: dict, target_date: date) -> dict:
    """Combine OHLCV bar + technical indicators into one record."""
    record = {
        "date":     target_date.isoformat(),
        "symbol":   symbol,
        "o":        float(bar["open"]),
        "h":        float(bar["high"]),
        "l":        float(bar["low"]),
        "c":        float(bar["close"]),
        "v":        int(bar["volume"]) if bar.get("volume") else None,
    }

    def get_daily_ind(ind: str, extra: dict = None) -> dict:
        p = {
            "interval": "1day", 
            "outputsize": 100,
            "start_date": target_date.isoformat(),
            "end_date": (target_date + timedelta(days=1)).isoformat()
        }
        if extra: p.update(extra)
        series = fetch_indicator_series(symbol, ind, p)
        return series.get(target_date.isoformat(), {})

    log.info("  Fetching RSI for %s ...", symbol)
    r = get_daily_ind("rsi", {"time_period": 14})
    record["rsi14"] = float(r.get("rsi", 0) or 0) if r else None

    log.info("  Fetching MACD for %s ...", symbol)
    m = get_daily_ind("macd")
    if m:
        record["macd"]        = float(m.get("macd", 0) or 0)
        record["macd_signal"] = float(m.get("macd_signal", 0) or 0)
        record["macd_hist"]   = float(m.get("macd_hist", 0) or 0)
    else:
        record.update({"macd": None, "macd_signal": None, "macd_hist": None})

    log.info("  Fetching Bollinger Bands for %s ...", symbol)
    bband = get_daily_ind("bbands")
    if bband:
        record["bb_upper"] = float(bband.get("upper_band", 0) or 0)
        record["bb_mid"]   = float(bband.get("middle_band", 0) or 0)
        record["bb_lower"] = float(bband.get("lower_band", 0) or 0)
    else:
        record.update({"bb_upper": None, "bb_mid": None, "bb_lower": None})

    log.info("  Fetching ATR for %s ...", symbol)
    a = get_daily_ind("atr", {"time_period": 14})
    record["atr14"] = float(a.get("atr", 0) or 0) if a else None

    record["fetched_at"] = datetime.utcnow().isoformat() + "Z"
    return record


# ── Minute data ───────────────────────────────────────────────────────────────

def fetch_minute_bars(symbol: str, target_date: date) -> list[dict]:
    """Fetch all 1-minute bars for a given date."""
    start_str = f"{target_date.isoformat()} 00:00:00"
    end_str   = f"{target_date.isoformat()} 23:59:59"

    data = get("time_series", {
        "symbol":     symbol,
        "interval":   "1min",
        "outputsize": 1500,
        "start_date": start_str,
        "end_date":   end_str,
        "format":     "JSON",
    })
    time.sleep(REQUEST_DELAY)

    if not data or "values" not in data:
        return []

    target_str = target_date.isoformat()
    bars = []
    for row in data["values"]:
        if row["datetime"].startswith(target_str):
            bars.append({
                "t": row["datetime"],
                "o": float(row["open"]),
                "h": float(row["high"]),
                "l": float(row["low"]),
                "c": float(row["close"]),
                "v": int(row["volume"]) if row.get("volume") else None,
            })

    # Return chronological order
    bars.sort(key=lambda x: x["t"])

    # --- Indicators ---
    def get_min_ind(ind: str, extra: dict = None) -> dict:
        p = {
            "interval":   "1min",
            "outputsize": 1500,
            "start_date": start_str,
            "end_date":   end_str,
        }
        if extra: p.update(extra)
        return fetch_indicator_series(symbol, ind, p)

    log.info("  Fetching minutely RSI for %s ...", symbol)
    rsi = get_min_ind("rsi", {"time_period": 14})
    log.info("  Fetching minutely MACD for %s ...", symbol)
    macd = get_min_ind("macd")
    log.info("  Fetching minutely Bollinger Bands for %s ...", symbol)
    bb = get_min_ind("bbands")
    log.info("  Fetching minutely ATR for %s ...", symbol)
    atr = get_min_ind("atr", {"time_period": 14})

    for b in bars:
        dt = b["t"]
        
        r = rsi.get(dt)
        b["rsi14"] = float(r.get("rsi", 0) or 0) if r else None
        
        m = macd.get(dt)
        if m:
            b["macd"] = float(m.get("macd", 0) or 0)
            b["macd_signal"] = float(m.get("macd_signal", 0) or 0)
            b["macd_hist"] = float(m.get("macd_hist", 0) or 0)
        else:
            b["macd"] = b["macd_signal"] = b["macd_hist"] = None
            
        bband = bb.get(dt)
        if bband:
            b["bb_upper"] = float(bband.get("upper_band", 0) or 0)
            b["bb_mid"] = float(bband.get("middle_band", 0) or 0)
            b["bb_lower"] = float(bband.get("lower_band", 0) or 0)
        else:
            b["bb_upper"] = b["bb_mid"] = b["bb_lower"] = None
            
        a = atr.get(dt)
        b["atr14"] = float(a.get("atr", 0) or 0) if a else None

    return bars


# ── File I/O ──────────────────────────────────────────────────────────────────

def safe_symbol_dir(symbol: str) -> str:
    """Convert symbol to filesystem-safe directory name."""
    return symbol.replace("/", "-")


def append_daily(record: dict) -> None:
    """Insert one record into the daily JSONL file, sorting by date."""
    symbol  = record["symbol"]
    dirname = safe_symbol_dir(symbol)
    path    = DATA_DIR / dirname / "daily.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)

    records = []
    target_date = record["date"]
    
    if path.exists():
        with open(path, "r") as f:
            for line in f:
                line = line.strip()
                if not line: continue
                try:
                    existing = json.loads(line)
                    if existing.get("date") == target_date:
                        old_comp = {k: v for k, v in existing.items() if k != "fetched_at"}
                        new_comp = {k: v for k, v in record.items() if k != "fetched_at"}
                        if old_comp == new_comp:
                            log.warning("Daily record for %s on %s is identical, skipping.", symbol, target_date)
                            return
                        else:
                            log.warning("Daily record for %s on %s differs, overwriting.", symbol, target_date)
                    else:
                        records.append(existing)
                except json.JSONDecodeError:
                    continue

    records.append(record)
    records.sort(key=lambda x: x.get("date", ""))

    with open(path, "w") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    log.info("  ✓ Saved daily record to %s (sorted)", path)


def write_minutes(symbol: str, target_date: date, bars: list[dict]) -> None:
    """Write minute bars to a per-day JSONL file."""
    if not bars:
        log.warning("No minute bars for %s on %s", symbol, target_date)
        return

    dirname  = safe_symbol_dir(symbol)
    filename = f"{target_date.isoformat()}.jsonl"
    path     = DATA_DIR / dirname / "minutes" / filename
    path.parent.mkdir(parents=True, exist_ok=True)

    if path.exists():
        try:
            with open(path, "r") as f:
                old_bars = [json.loads(line) for line in f if line.strip()]
            if old_bars == bars:
                log.warning("Minute file %s already exists and is identical, skipping.", path)
                return
        except Exception:
            pass
        log.warning("Minute file %s already exists and differs, overwriting.", path)

    with open(path, "w") as f:
        for bar in bars:
            f.write(json.dumps(bar, ensure_ascii=False) + "\n")

    log.info("  ✓ Wrote %d minute bars → %s", len(bars), path)


# ── Last trading day helper ───────────────────────────────────────────────────

def last_trading_day(ref: date = None) -> date:
    d = (ref or date.today()) - timedelta(days=1)
    return d


# ── Main ──────────────────────────────────────────────────────────────────────

def run(target_date: date = None) -> None:
    target = target_date or last_trading_day()
    minute_date = target

    log.info("=" * 60)
    log.info("Fetching data  daily=%s  minutes=%s", target, minute_date)
    log.info("=" * 60)

    for symbol, meta in SYMBOLS.items():
        log.info("── %s ──────────────────────────────────────────", symbol)

        # 1. Daily bar + indicators
        log.info("Fetching daily bar ...")
        bar = fetch_daily_bar(symbol, target)
        if bar:
            record = build_daily_record(symbol, bar, target)
            log.info(record)
            append_daily(record)
        else:
            log.error("Could not fetch daily bar for %s", symbol)

        # 2. Minute bars for previous trading day
        log.info("Fetching minute bars for %s ...", minute_date)
        bars = fetch_minute_bars(symbol, minute_date)
        write_minutes(symbol, minute_date, bars)

    log.info("Done.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Fetch market data from Twelve Data")
    parser.add_argument(
        "--date",
        type=lambda s: date.fromisoformat(s),
        default=None,
        help="Target date for daily bar (YYYY-MM-DD). Defaults to last trading day.",
    )
    args = parser.parse_args()
    run(target_date=args.date)
