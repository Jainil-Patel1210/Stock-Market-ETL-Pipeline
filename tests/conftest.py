"""Shared fixtures. The Spark pipeline fixture runs the real Glue scripts locally on synthetic data."""
import csv
import datetime as dt
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
GLUE_DIR = ROOT / "aws" / "glue"

HEADER = ["Date", "Open", "High", "Low", "Close", "Volume", "Ticker"]
N_DAYS = 40  # trading days per ticker in the synthetic data


def weekdays(start: dt.date, n: int):
    out, d = [], start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += dt.timedelta(days=1)
    return out


def price_rows(ticker: str, base: float):
    """Deterministic prices that move up and down, so returns and volatility are non-trivial."""
    rows = []
    for i, d in enumerate(weekdays(dt.date(2026, 6, 1), N_DAYS)):
        close = base + 0.5 * i + (i % 5)
        rows.append([d.isoformat(), close, close + 1, close - 1, close, 1000 + 10 * i, ticker])
    return rows


def write_csv(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(HEADER)
        writer.writerows(rows)


def run_job(script: str, *args: str) -> str:
    env = dict(os.environ, PYSPARK_PYTHON=sys.executable, SPARK_LOCAL_IP="127.0.0.1")
    proc = subprocess.run([sys.executable, str(GLUE_DIR / script), *args],
                          capture_output=True, text=True, env=env, timeout=600)
    if proc.returncode != 0:
        raise RuntimeError(f"{script} failed ({proc.returncode})\n--- stdout ---\n{proc.stdout[-3000:]}"
                           f"\n--- stderr ---\n{proc.stderr[-3000:]}")
    return proc.stdout


def read_metrics(metrics_dir: Path) -> dict:
    files = sorted(metrics_dir.glob("*.json"))
    assert files, f"no metrics file in {metrics_dir}"
    return json.loads(files[0].read_text().strip().splitlines()[0])


@pytest.fixture(scope="session")
def pipeline(tmp_path_factory):
    """Bronze -> Silver (twice) -> Gold on synthetic data. Returns paths and run metrics."""
    if shutil.which("java") is None:
        pytest.skip("Java not found; local Spark tests need a JDK on PATH")
    if sys.platform == "win32" and not os.environ.get("HADOOP_HOME"):
        pytest.skip("local Spark writes on Windows need HADOOP_HOME (winutils); run these tests on Linux/WSL")

    root = tmp_path_factory.mktemp("pipeline")
    bronze, silver, quarantine = root / "bronze", root / "silver", root / "quarantine"
    metrics, gold = root / "metrics", root / "gold"

    aapl, msft, zzzz = price_rows("AAPL", 100.0), price_rows("MSFT", 200.0), price_rows("ZZZZ", 50.0)
    bad_rows = [
        ["2026-06-01", 1, 2, 1, "", 5, "BADCLOSE"],      # null close
        ["2026-06-01", 1, 1, 5, 3, 5, "HILOW"],          # high below low
        ["2026-06-01", 1, 2, 1, 3, -5, "NEGVOL"],        # negative volume
        ["notadate", 1, 2, 1, 3, 5, "BADDATE"],          # unparseable date
    ]
    full_load = aapl + msft + zzzz + bad_rows
    write_csv(bronze / "ingest_date=2026-11-01" / "raw_stock_data.csv", full_load)

    # A later incremental file re-sends the last 3 AAPL days with restated (doubled) closes.
    restated = [r[:] for r in aapl[-3:]]
    for r in restated:
        r[4] = r[4] * 2
    write_csv(bronze / "ingest_date=2026-11-02" / "raw_stock_data_incremental.csv", restated)

    silver_args = ["--bronze_path", str(bronze), "--silver_path", str(silver),
                   "--quarantine_path", str(quarantine), "--metrics_path", str(metrics)]
    run_job("silver_job.py", *silver_args)
    first_metrics = read_metrics(metrics)
    run_job("silver_job.py", *silver_args)  # second run on identical Bronze: must be idempotent
    second_metrics = read_metrics(metrics)
    run_job("gold_job.py", "--silver_path", str(silver), "--gold_path", str(gold))

    return {
        "silver": silver, "quarantine": quarantine, "gold": gold,
        "first_metrics": first_metrics, "second_metrics": second_metrics,
        "raw_row_count": len(full_load) + len(restated),
        "restated_closes": {r[0]: r[4] for r in restated},
    }
