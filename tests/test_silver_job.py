import pandas as pd
import pytest

pytestmark = pytest.mark.spark


def read_silver(pipeline):
    df = pd.read_parquet(pipeline["silver"])
    df["year"] = df["year"].astype(int)
    return df


def test_bad_rows_are_quarantined_with_a_reason(pipeline):
    quarantine = pd.read_parquet(pipeline["quarantine"])
    reasons = dict(zip(quarantine["ticker"], quarantine["reject_reason"]))
    assert reasons == {
        "BADDATE": "null_or_unparseable_date",
        "BADCLOSE": "null_close",
        "HILOW": "high_below_low",
        "NEGVOL": "negative_volume",
    }


def test_rejected_tickers_never_reach_silver(pipeline):
    silver = read_silver(pipeline)
    assert set(silver["ticker"]) == {"AAPL", "MSFT", "ZZZZ"}


def test_no_duplicate_ticker_date_pairs(pipeline):
    silver = read_silver(pipeline)
    assert not silver.duplicated(["ticker", "trade_date"]).any()


def test_newest_ingest_wins_for_overlapping_days(pipeline):
    silver = read_silver(pipeline)
    aapl = silver[silver["ticker"] == "AAPL"].copy()
    aapl["day"] = aapl["trade_date"].astype(str)
    aapl = aapl.set_index("day")
    for day, restated_close in pipeline["restated_closes"].items():
        assert aapl.loc[day, "close"] == pytest.approx(restated_close)
        assert aapl.loc[day, "ingest_date"].isoformat() == "2026-11-02"


def test_moving_average_is_empty_until_a_full_window_exists(pipeline):
    silver = read_silver(pipeline)
    msft = silver[silver["ticker"] == "MSFT"].sort_values("trade_date").reset_index(drop=True)
    assert msft["moving_avg_7d"].iloc[:6].isna().all()
    expected = msft["close"].rolling(7).mean().round(4)
    pd.testing.assert_series_equal(msft["moving_avg_7d"].iloc[6:], expected.iloc[6:], check_names=False)


def test_daily_return_matches_percent_change(pipeline):
    silver = read_silver(pipeline)
    msft = silver[silver["ticker"] == "MSFT"].sort_values("trade_date").reset_index(drop=True)
    assert pd.isna(msft["daily_return"].iloc[0])
    expected = msft["close"].pct_change().round(6)
    pd.testing.assert_series_equal(msft["daily_return"].iloc[1:], expected.iloc[1:], check_names=False)


def test_metrics_describe_the_first_run(pipeline):
    m = pipeline["first_metrics"]
    assert m["raw_rows"] == pipeline["raw_row_count"]
    assert m["rejected_rows"] == 4
    assert m["duplicates_removed"] == 3  # the three re-sent AAPL days
    assert m["silver_rows"] == m["distinct_keys"] == 3 * 40
    assert m["ticker_count"] == 3
    assert "previous_silver_rows" not in m  # nothing existed before the first run


def test_rerunning_on_the_same_bronze_is_idempotent(pipeline):
    first, second = pipeline["first_metrics"], pipeline["second_metrics"]
    assert second["silver_rows"] == first["silver_rows"]
    assert second["previous_silver_rows"] == first["silver_rows"]
