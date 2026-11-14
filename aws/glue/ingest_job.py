"""Bronze ingestion as an AWS Glue job: pull recent OHLCV data from yfinance and land it,
untouched, in S3.

Writes   s3://<bucket>/bronze/stocks/ingest_date=YYYY-MM-DD/raw_stock_data_incremental.csv

By default it pulls only the last --lookback_days days. The Silver job de-duplicates across
ingests (newest wins), so overlapping windows are safe. Re-running on the same day overwrites
only this job's own incremental file, never a full-history file in the same folder.

Glue job parameters:
    --additional-python-modules   yfinance        (Glue installs it from PyPI at job start)
    --bucket                      <your bucket>
    --lookback_days               10              (optional)
    --start                       YYYY-MM-DD      (optional, overrides the lookback)
"""
import argparse
from datetime import date, timedelta

import boto3
import pandas as pd
import yfinance as yf

TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META",
    "TSLA", "NVDA", "JPM", "V", "WMT",
    "JNJ", "PG", "UNH", "HD", "DIS",
    "KO", "PEP", "XOM", "CVX", "BA",
]
COLUMNS = ["Date", "Open", "High", "Low", "Close", "Volume", "Ticker"]


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--lookback_days", type=int, default=10)
    parser.add_argument("--start", default=None)
    args, _ = parser.parse_known_args()  # Glue adds its own arguments, so ignore unknown ones
    return args


def download(start: str, end: str) -> pd.DataFrame:
    """Download all tickers and reshape from wide (MultiIndex columns) to long."""
    data = yf.download(
        TICKERS, start=start, end=end,
        group_by="ticker", auto_adjust=True, progress=False,
    )
    available = set(data.columns.get_level_values(0))

    # Fail loudly if any ticker is missing. A silent gap would leave that ticker's
    # newest data stale while the rest of the pipeline still looks healthy.
    missing = [t for t in TICKERS if t not in available]
    if missing:
        raise RuntimeError(f"yfinance returned no data for: {', '.join(missing)}")

    frames = []
    for ticker in TICKERS:
        df = data[ticker].copy()
        df["Ticker"] = ticker
        frames.append(df.reset_index())

    return pd.concat(frames, ignore_index=True)[COLUMNS]


def main():
    args = parse_args()
    today = date.today()
    start = args.start or (today - timedelta(days=args.lookback_days)).isoformat()
    end = today.isoformat()  # exclusive, so today's unfinished trading day is never included

    df = download(start, end)
    df = df.dropna(how="all", subset=["Open", "High", "Low", "Close", "Volume"])
    if df.empty:
        raise RuntimeError(f"No rows between {start} and {end}")

    # Distinct file name: this job must never overwrite a full-history file (raw_stock_data.csv)
    # that was loaded into the same ingest_date folder.
    key = f"bronze/stocks/ingest_date={today.isoformat()}/raw_stock_data_incremental.csv"
    boto3.client("s3").put_object(
        Bucket=args.bucket,
        Key=key,
        Body=df.to_csv(index=False).encode("utf-8"),
        ContentType="text/csv",
    )
    print(f"INGEST_SUMMARY rows={len(df)} tickers={df['Ticker'].nunique()} "
          f"from={df['Date'].min():%Y-%m-%d} to={df['Date'].max():%Y-%m-%d} "
          f"target=s3://{args.bucket}/{key}")


if __name__ == "__main__":
    main()
