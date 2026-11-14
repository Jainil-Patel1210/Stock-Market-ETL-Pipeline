import importlib
import sys
import types
from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

FIELDS = ["Open", "High", "Low", "Close", "Volume"]


@pytest.fixture
def ingest(monkeypatch):
    """Import ingest_job with yfinance and boto3 replaced by fakes, so no network or AWS is touched."""
    state = {"tickers_returned": None, "download_args": None, "puts": []}

    def fake_download(tickers, start, end, group_by, auto_adjust, progress):
        state["download_args"] = {"start": start, "end": end}
        returned = state["tickers_returned"] or tickers
        idx = pd.date_range("2026-10-26", "2026-10-30", freq="B", name="Date")
        cols = pd.MultiIndex.from_product([returned, FIELDS])
        values = np.arange(len(idx) * len(cols), dtype=float).reshape(len(idx), len(cols)) + 100
        return pd.DataFrame(values, index=idx, columns=cols)

    class FakeS3:
        def put_object(self, **kwargs):
            state["puts"].append(kwargs)

    fake_yf = types.ModuleType("yfinance")
    fake_yf.download = fake_download
    fake_boto3 = types.ModuleType("boto3")
    fake_boto3.client = lambda name: FakeS3()
    monkeypatch.setitem(sys.modules, "yfinance", fake_yf)
    monkeypatch.setitem(sys.modules, "boto3", fake_boto3)
    monkeypatch.delitem(sys.modules, "ingest_job", raising=False)
    module = importlib.import_module("ingest_job")
    yield module, state
    sys.modules.pop("ingest_job", None)


def run(module, monkeypatch, *args):
    # --JOB_NAME mimics the extra arguments Glue always passes
    monkeypatch.setattr(sys, "argv", ["ingest_job.py", "--bucket", "my-bucket", "--JOB_NAME", "x", *args])
    module.main()


def test_uploads_one_incremental_file_with_the_expected_layout(ingest, monkeypatch):
    module, state = ingest
    run(module, monkeypatch)

    assert len(state["puts"]) == 1
    put = state["puts"][0]
    assert put["Bucket"] == "my-bucket"
    assert put["Key"].startswith(f"bronze/stocks/ingest_date={date.today().isoformat()}/")
    lines = put["Body"].decode().splitlines()
    assert lines[0] == "Date,Open,High,Low,Close,Volume,Ticker"
    assert len(lines) - 1 == 5 * len(module.TICKERS)


def test_never_overwrites_the_full_history_file(ingest, monkeypatch):
    """Regression: a 10-day pull once overwrote raw_stock_data.csv and wiped the history."""
    module, state = ingest
    run(module, monkeypatch)
    assert state["puts"][0]["Key"].endswith("raw_stock_data_incremental.csv")
    assert not state["puts"][0]["Key"].endswith("/raw_stock_data.csv")


def test_default_lookback_window_ends_today_exclusive(ingest, monkeypatch):
    module, state = ingest
    run(module, monkeypatch)
    assert state["download_args"] == {
        "start": (date.today() - timedelta(days=10)).isoformat(),
        "end": date.today().isoformat(),
    }


def test_start_argument_overrides_the_lookback(ingest, monkeypatch):
    module, state = ingest
    run(module, monkeypatch, "--start", "2019-01-01")
    assert state["download_args"]["start"] == "2019-01-01"


def test_missing_ticker_fails_loudly_and_uploads_nothing(ingest, monkeypatch):
    module, state = ingest
    state["tickers_returned"] = ["AAPL", "MSFT"]
    with pytest.raises(RuntimeError, match="no data for"):
        run(module, monkeypatch)
    assert state["puts"] == []
