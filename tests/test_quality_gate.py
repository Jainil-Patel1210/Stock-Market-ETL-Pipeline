import io
from datetime import date

import pytest

import quality_gate as qg

TODAY = date(2026, 11, 2)
LIMITS = dict(max_reject_pct=0.01, min_tickers=20, max_staleness_days=5, max_shrink_pct=0.01)


def healthy(**overrides):
    metrics = {
        "raw_rows": 39380, "rejected_rows": 0, "duplicates_removed": 0, "silver_rows": 39380,
        "previous_silver_rows": 39380, "distinct_keys": 39380, "ticker_count": 20,
        "min_trade_date": "2019-01-02", "max_trade_date": "2026-10-30",
    }
    metrics.update(overrides)
    return metrics


def failures(**overrides):
    return qg.evaluate(healthy(**overrides), today=TODAY, **LIMITS)


def test_healthy_data_passes():
    assert failures() == []


def test_first_run_without_previous_count_passes():
    metrics = healthy()
    del metrics["previous_silver_rows"]
    assert qg.evaluate(metrics, today=TODAY, **LIMITS) == []


def test_empty_silver_fails():
    assert any("empty" in f for f in failures(silver_rows=0, distinct_keys=0, previous_silver_rows=None))


def test_high_reject_rate_fails():
    assert any("Reject rate" in f for f in failures(rejected_rows=500))


def test_reject_rate_within_limit_passes():
    assert failures(rejected_rows=100) == []  # 0.25% of 39,380


def test_duplicate_keys_fail():
    assert any("Duplicate" in f for f in failures(distinct_keys=39000))


def test_too_few_tickers_fail():
    assert any("tickers" in f for f in failures(ticker_count=19))


def test_stale_data_fails():
    assert any("days old" in f for f in failures(max_trade_date="2026-10-20"))


def test_weekend_gap_is_not_stale():
    # Friday close checked on Tuesday morning is 4 days old, inside the 5-day limit
    assert qg.evaluate(healthy(max_trade_date="2026-10-30"), today=date(2026, 11, 3), **LIMITS) == []


def test_shrinking_silver_fails():
    # The real incident: a 10-day file overwrote the full history, so Silver fell from 39,380 to 140 rows.
    result = failures(silver_rows=140, distinct_keys=140, previous_silver_rows=39380, raw_rows=140)
    assert any("shrank" in f for f in result)


def test_tiny_shrink_within_tolerance_passes():
    assert failures(silver_rows=39300, distinct_keys=39300, raw_rows=39300) == []


def test_growth_passes():
    assert failures(silver_rows=39400, distinct_keys=39400, raw_rows=39400) == []


class FakeS3:
    def __init__(self, objects):
        self.objects = objects

    def list_objects_v2(self, Bucket, Prefix):
        return {"Contents": [{"Key": k} for k in self.objects if k.startswith(Prefix)]}

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(self.objects[Key])}


def test_load_metrics_reads_the_json_file_and_ignores_other_objects(monkeypatch):
    fake = FakeS3({
        "silver_metrics/stocks/_SUCCESS": b"",
        "silver_metrics/stocks/part-0000.json": b'{"silver_rows": 7}\n',
    })
    monkeypatch.setattr(qg, "s3", fake)
    assert qg.load_metrics("bucket", "silver_metrics/stocks/") == {"silver_rows": 7}


def test_load_metrics_raises_when_no_file_exists(monkeypatch):
    monkeypatch.setattr(qg, "s3", FakeS3({"silver_metrics/stocks/_SUCCESS": b""}))
    with pytest.raises(FileNotFoundError):
        qg.load_metrics("bucket", "silver_metrics/stocks/")


def test_handler_reports_failures_and_honours_env_thresholds(monkeypatch):
    import json
    body = json.dumps(healthy(ticker_count=22, max_trade_date=date.today().isoformat())).encode()
    monkeypatch.setattr(qg, "s3", FakeS3({"p/part.json": body}))
    event = {"bucket": "b", "prefix": "p/"}

    assert qg.lambda_handler(event, None)["passed"] is True

    monkeypatch.setenv("MIN_TICKERS", "25")
    result = qg.lambda_handler(event, None)
    assert result["passed"] is False
    assert any("tickers" in f for f in result["failures"])
