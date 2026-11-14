"""Data quality gate (AWS Lambda).

Reads the metrics JSON that the Silver job writes and decides whether the data is
good enough to build the Gold layer from. Uses only boto3, so no layers are needed.

Event:   {"bucket": "<bucket>", "prefix": "silver_metrics/stocks/"}
Returns: {"passed": bool, "failures": [str, ...], "metrics": {...}}

Thresholds come from Lambda environment variables (defaults in brackets):
    MAX_REJECT_PCT        [0.01]  max share of raw rows that may be rejected
    MIN_TICKERS           [20]    minimum number of distinct tickers
    MAX_STALENESS_DAYS    [5]     max age of the newest trade date, in calendar days
    MAX_SHRINK_PCT        [0.01]  max allowed drop in Silver row count versus the previous run
"""
import json
import os
from datetime import date, datetime, timezone

import boto3

s3 = boto3.client("s3")


def load_metrics(bucket: str, prefix: str) -> dict:
    listing = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    keys = [o["Key"] for o in listing.get("Contents", []) if o["Key"].endswith(".json")]
    if not keys:
        raise FileNotFoundError(f"No metrics file found under s3://{bucket}/{prefix}")
    body = s3.get_object(Bucket=bucket, Key=sorted(keys)[0])["Body"].read().decode("utf-8")
    return json.loads(body.strip().splitlines()[0])


def evaluate(metrics: dict, today: date, max_reject_pct: float, min_tickers: int,
             max_staleness_days: int, max_shrink_pct: float = 0.01) -> list:
    """Return a list of human-readable failures. An empty list means the data passed."""
    failures = []

    if metrics["silver_rows"] <= 0:
        failures.append("Silver output is empty")

    # Market history only grows. A big drop versus the previous run means data was lost.
    previous = metrics.get("previous_silver_rows")
    if previous and metrics["silver_rows"] < previous * (1 - max_shrink_pct):
        failures.append(
            f"Silver shrank from {previous} to {metrics['silver_rows']} rows "
            f"(allowed shrink is {max_shrink_pct:.1%})"
        )

    raw = metrics["raw_rows"]
    reject_pct = metrics["rejected_rows"] / raw if raw else 1.0
    if reject_pct > max_reject_pct:
        failures.append(f"Reject rate {reject_pct:.2%} is above the {max_reject_pct:.2%} limit")

    if metrics["silver_rows"] != metrics["distinct_keys"]:
        failures.append("Duplicate (ticker, trade_date) rows remain in Silver")

    if metrics["ticker_count"] < min_tickers:
        failures.append(f"Only {metrics['ticker_count']} tickers present, expected at least {min_tickers}")

    newest = date.fromisoformat(metrics["max_trade_date"])
    age = (today - newest).days
    if age > max_staleness_days:
        failures.append(f"Newest trade date {newest} is {age} days old, limit is {max_staleness_days}")

    return failures


def lambda_handler(event, context):
    metrics = load_metrics(event["bucket"], event["prefix"])
    failures = evaluate(
        metrics,
        today=datetime.now(timezone.utc).date(),
        max_reject_pct=float(os.environ.get("MAX_REJECT_PCT", "0.01")),
        min_tickers=int(os.environ.get("MIN_TICKERS", "20")),
        max_staleness_days=int(os.environ.get("MAX_STALENESS_DAYS", "5")),
        max_shrink_pct=float(os.environ.get("MAX_SHRINK_PCT", "0.01")),
    )
    result = {"passed": not failures, "failures": failures, "metrics": metrics}
    print(json.dumps(result))
    return result
