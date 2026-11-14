"""Bronze ingestion: pull raw OHLCV data from yfinance and land it, untouched, in S3.

Layout:
    s3://<BUCKET_NAME>/bronze/stocks/ingest_date=YYYY-MM-DD/raw_stock_data.csv

Bronze is the raw layer, so no cleaning happens here. Validation and
de-duplication belong to the Silver step. Re-running on the same day
overwrites the same key, so the upload is idempotent.

Usage:
    python fetch_data.py                      # full history -> S3
    python fetch_data.py --start 2026-10-01   # smaller window -> S3
    python fetch_data.py --local              # skip AWS, write ./data/raw_stock_data.csv
"""
import argparse
import os
from datetime import date

import boto3
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv

load_dotenv()

TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META",
    "TSLA", "NVDA", "JPM", "V", "WMT",
    "JNJ", "PG", "UNH", "HD", "DIS",
    "KO", "PEP", "XOM", "CVX", "BA",
]
DEFAULT_START = "2019-01-01"
COLUMNS = ["Date", "Open", "High", "Low", "Close", "Volume", "Ticker"]


def download(start: str, end: str) -> pd.DataFrame:
    """Download all tickers and reshape from wide (MultiIndex columns) to long."""
    data = yf.download(
        TICKERS, start=start, end=end,
        group_by="ticker", auto_adjust=True, progress=False,
    )
    available = set(data.columns.get_level_values(0))

    frames = []
    for ticker in TICKERS:
        if ticker not in available:
            print(f"WARNING: no data returned for {ticker}, skipping")
            continue
        df = data[ticker].copy()
        df["Ticker"] = ticker
        frames.append(df.reset_index())

    if not frames:
        raise RuntimeError("yfinance returned no data for any ticker")

    return pd.concat(frames, ignore_index=True)[COLUMNS]


def upload_to_bronze(df: pd.DataFrame, bucket: str, ingest_date: str, region: str) -> str:
    key = f"bronze/stocks/ingest_date={ingest_date}/raw_stock_data.csv"
    s3 = boto3.client("s3", region_name=region)
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=df.to_csv(index=False).encode("utf-8"),
        ContentType="text/csv",
    )
    return f"s3://{bucket}/{key}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest raw stock data into the Bronze layer")
    parser.add_argument("--start", default=DEFAULT_START, help="first date to pull (YYYY-MM-DD)")
    parser.add_argument("--end", default=date.today().isoformat(),
                        help="end date, exclusive (YYYY-MM-DD); defaults to today")
    parser.add_argument("--local", action="store_true",
                        help="write ./data/raw_stock_data.csv instead of uploading to S3")
    args = parser.parse_args()

    df = download(args.start, args.end)
    print(f"Downloaded {len(df):,} rows for {df['Ticker'].nunique()} tickers "
          f"({df['Date'].min():%Y-%m-%d} to {df['Date'].max():%Y-%m-%d})")

    if args.local:
        os.makedirs("data", exist_ok=True)
        df.to_csv("data/raw_stock_data.csv", index=False)
        print("Saved locally: data/raw_stock_data.csv")
        return

    bucket = os.getenv("BUCKET_NAME")
    if not bucket:
        raise SystemExit("BUCKET_NAME is not set. Add it to your .env file.")
    region = os.getenv("AWS_REGION", "ap-southeast-2")

    uri = upload_to_bronze(df, bucket, date.today().isoformat(), region)
    print(f"Uploaded to {uri}")


if __name__ == "__main__":
    main()
