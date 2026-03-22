# Market Dataset — AAPL & BTC/USD

Daily-updated financial dataset built for **LLM fine-tuning, world model training, and inference benchmarking**.

Data is fetched automatically from [Twelve Data](https://twelvedata.com) after each NYSE market close and committed to this repo via GitHub Actions.

---

## Structure

```
data/
├── AAPL/
│   ├── daily.jsonl          ← one record per trading day (growing)
│   └── minutes/
│       ├── 2024-01-13.jsonl ← ~390 1-min bars per file
│       ├── 2024-01-14.jsonl
│       └── ...
└── BTC-USD/
    ├── daily.jsonl          ← one record per day (growing)
    └── minutes/
        ├── 2024-01-13.jsonl ← 1440 1-min bars (24h crypto)
        └── ...
```

Each `git commit` corresponds to exactly one trading day.  
The git log is a queryable timeline — `git log --oneline` gives you the full calendar.

---

## Daily record fields

| Field | Description |
|---|---|
| `date` | YYYY-MM-DD |
| `symbol` | AAPL or BTC/USD |
| `o h l c` | Open / High / Low / Close |
| `v` | Volume |
| `rsi14` | RSI (14-period) |
| `macd` / `macd_signal` / `macd_hist` | MACD (12/26/9) |
| `bb_upper` / `bb_mid` / `bb_lower` | Bollinger Bands (20-period, 2σ) |
| `atr14` | Average True Range (14-period) |
| `fetched_at` | UTC timestamp of fetch |

Full schema → [`schema.json`](./schema.json)

---

## Quickstart

```python
import json
import pandas as pd

# Load full daily history
with open("data/AAPL/daily.jsonl") as f:
    records = [json.loads(line) for line in f]

df = pd.DataFrame(records)
df["date"] = pd.to_datetime(df["date"])
df = df.set_index("date").sort_index()

print(df[["c", "rsi14", "macd"]].tail())
```

```python
# Load one day of minute bars
with open("data/AAPL/minutes/2024-01-15.jsonl") as f:
    bars = [json.loads(line) for line in f]

df_min = pd.DataFrame(bars)
df_min["t"] = pd.to_datetime(df_min["t"])
```

---

## Running locally

```bash
pip install -r requirements.txt

# Use your own key (or leave as demo for testing)
export TWELVEDATA_API_KEY=your_key_here

# Fetch last trading day
python fetch.py

# Fetch a specific date
python fetch.py --date 2024-01-15

# Validate all data files
python validate.py
```

---

## GitHub Actions setup

1. Fork this repo
2. Add your Twelve Data API key as a secret: `Settings → Secrets → TWELVEDATA_API_KEY`
3. The workflow runs automatically at **21:30 UTC Mon–Fri**

---

## Notes for ML use

- **Train/val/test split**: suggested 2015–2021 / 2022 / 2023–present
- Indicators are pre-computed to save repeated fetching
- Minute data enables intraday world model simulation
- `null` values occur when the API lacks sufficient history for an indicator — handle gracefully
- Stock splits are **not** retroactively adjusted — factor this into your preprocessing

---

## License

Data sourced from Twelve Data. See their [Terms of Service](https://twelvedata.com/terms).  
Code in this repo: MIT.
