import math

import pandas as pd
import pytest

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def silver(pipeline):
    df = pd.read_parquet(pipeline["silver"])
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    return df.sort_values(["ticker", "trade_date"])


@pytest.fixture(scope="module")
def ticker_summary(pipeline):
    return pd.read_parquet(pipeline["gold"] / "ticker_summary").set_index("ticker")


@pytest.fixture(scope="module")
def monthly(pipeline):
    return pd.read_parquet(pipeline["gold"] / "monthly_ticker_stats")


@pytest.fixture(scope="module")
def sector(pipeline):
    return pd.read_parquet(pipeline["gold"] / "sector_monthly_stats")


def test_one_summary_row_per_ticker(ticker_summary):
    assert sorted(ticker_summary.index) == ["AAPL", "MSFT", "ZZZZ"]
    assert (ticker_summary["trading_days"] == 40).all()


def test_total_return_and_drawdown_match_pandas(silver, ticker_summary):
    for ticker, group in silver.groupby("ticker"):
        close = group["close"].reset_index(drop=True)
        row = ticker_summary.loc[ticker]
        assert row["total_return"] == pytest.approx(close.iloc[-1] / close.iloc[0] - 1, abs=1e-4)
        assert row["max_drawdown"] == pytest.approx((close / close.cummax() - 1).min(), abs=1e-4)


def test_annualized_volatility_is_daily_std_times_sqrt_252(silver, ticker_summary):
    for ticker, group in silver.groupby("ticker"):
        expected = group["daily_return"].std(ddof=1) * math.sqrt(252)
        assert ticker_summary.loc[ticker, "annualized_vol"] == pytest.approx(expected, abs=1e-4)


def test_monthly_return_matches_month_end_close_ratio(silver, monthly):
    for ticker, group in silver.groupby("ticker"):
        month_end = group.groupby(group["trade_date"].dt.to_period("M"))["close"].last()
        expected = (month_end / month_end.shift(1) - 1).dropna()
        got = monthly[monthly["ticker"] == ticker].set_index("month")["monthly_return"]
        for period, value in expected.items():
            assert got[period.month] == pytest.approx(value, abs=5e-5)


def test_sector_table_uses_the_mapping_and_flags_unknown_tickers(sector):
    assert set(sector["sector"]) == {"Technology", "Unknown"}
    technology = sector[sector["sector"] == "Technology"]
    assert (technology["tickers"] == 2).all()  # AAPL and MSFT
    assert (sector[sector["sector"] == "Unknown"]["tickers"] == 1).all()  # ZZZZ is not in the mapping


def test_sector_average_is_the_mean_of_ticker_monthly_returns(monthly, sector):
    tech = monthly[monthly["ticker"].isin(["AAPL", "MSFT"])].groupby("month")["monthly_return"].mean()
    got = sector[sector["sector"] == "Technology"].set_index("month")["avg_monthly_return"]
    for month, value in tech.items():
        assert got[month] == pytest.approx(value, abs=1e-5)
