"""Gold layer: business-level aggregations built from the clean Silver data.

Reads    <silver_path>   (Parquet, partitioned by year)
Writes   <gold_path>/ticker_summary/          one row per ticker, whole-history stats
         <gold_path>/monthly_ticker_stats/    one row per ticker per month (partitioned by year)
         <gold_path>/sector_monthly_stats/    one row per sector per month (partitioned by year)

Like the Silver job, it recomputes everything and overwrites, so re-running is safe.

Runs unchanged as an AWS Glue job or locally:
    python gold_job.py --silver_path ./silver --gold_path ./gold
"""
import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

TRADING_DAYS_PER_YEAR = 252

# Small reference table: which sector each ticker belongs to.
SECTORS = {
    "AAPL": "Technology", "MSFT": "Technology", "NVDA": "Technology",
    "GOOGL": "Communication Services", "META": "Communication Services", "DIS": "Communication Services",
    "AMZN": "Consumer Discretionary", "TSLA": "Consumer Discretionary", "HD": "Consumer Discretionary",
    "WMT": "Consumer Staples", "PG": "Consumer Staples", "KO": "Consumer Staples", "PEP": "Consumer Staples",
    "JPM": "Financials", "V": "Financials",
    "JNJ": "Health Care", "UNH": "Health Care",
    "XOM": "Energy", "CVX": "Energy",
    "BA": "Industrials",
}


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver_path", required=True)
    parser.add_argument("--gold_path", required=True)
    args, _ = parser.parse_known_args()  # Glue adds its own arguments, so ignore unknown ones
    return args


def annualized_vol(col):
    """Annualized volatility from daily returns: sample std-dev times sqrt(252)."""
    return F.round(F.stddev_samp(col) * F.sqrt(F.lit(float(TRADING_DAYS_PER_YEAR))), 4)


def main():
    args = parse_args()
    gold = args.gold_path.rstrip("/")
    spark = SparkSession.builder.appName("stock_etl_gold").getOrCreate()

    silver = spark.read.parquet(args.silver_path).cache()

    # ------------------------------------------------------------------ ticker_summary
    # Drawdown = how far the close sits below its highest close so far; max_drawdown is the worst.
    running = Window.partitionBy("ticker").orderBy("trade_date").rowsBetween(Window.unboundedPreceding, 0)
    with_drawdown = silver.withColumn(
        "drawdown", F.col("close") / F.max("close").over(running) - 1
    )

    ticker_summary = (
        with_drawdown.groupBy("ticker")
        .agg(
            F.min("trade_date").alias("first_trade_date"),
            F.max("trade_date").alias("last_trade_date"),
            F.count("*").alias("trading_days"),
            F.min_by("close", "trade_date").alias("first_close"),
            F.max_by("close", "trade_date").alias("last_close"),
            annualized_vol("daily_return").alias("annualized_vol"),
            F.round(F.min("drawdown"), 4).alias("max_drawdown"),
            F.round(F.avg("volume"), 0).alias("avg_daily_volume"),
            F.max("daily_return").alias("best_day_return"),
            F.min("daily_return").alias("worst_day_return"),
        )
        .withColumn("total_return", F.round(F.col("last_close") / F.col("first_close") - 1, 4))
        .select(
            "ticker", "first_trade_date", "last_trade_date", "trading_days",
            "first_close", "last_close", "total_return", "annualized_vol",
            "max_drawdown", "avg_daily_volume", "best_day_return", "worst_day_return",
        )
    )

    # ------------------------------------------------------------- monthly_ticker_stats
    # Monthly return = daily returns compounded over the month: exp(sum(ln(1 + r))) - 1.
    monthly = (
        silver
        .withColumn("month", F.month("trade_date"))
        .groupBy("ticker", "year", "month")
        .agg(
            F.count("*").alias("trading_days"),
            F.round(F.exp(F.sum(F.log1p("daily_return"))) - 1, 6).alias("monthly_return"),
            F.max_by("close", "trade_date").alias("month_end_close"),
            F.round(F.avg("close"), 4).alias("avg_close"),
            F.sum("volume").alias("total_volume"),
            annualized_vol("daily_return").alias("annualized_vol"),
        )
        .select(
            "ticker", "month", "trading_days", "monthly_return", "month_end_close",
            "avg_close", "total_volume", "annualized_vol", "year",
        )
    )

    # ------------------------------------------------------------ sector_monthly_stats
    sector_dim = spark.createDataFrame(list(SECTORS.items()), ["ticker", "sector"])
    sector_monthly = (
        monthly.join(F.broadcast(sector_dim), "ticker", "left")
        .withColumn("sector", F.coalesce(F.col("sector"), F.lit("Unknown")))
        .groupBy("sector", "year", "month")
        .agg(
            F.countDistinct("ticker").alias("tickers"),
            F.round(F.avg("monthly_return"), 6).alias("avg_monthly_return"),
            F.round(F.avg("annualized_vol"), 4).alias("avg_annualized_vol"),
        )
        .select("sector", "month", "tickers", "avg_monthly_return", "avg_annualized_vol", "year")
    )

    # ------------------------------------------------------------------------- write
    ticker_summary.coalesce(1).write.mode("overwrite").parquet(f"{gold}/ticker_summary/")
    monthly.repartition("year").write.mode("overwrite").partitionBy("year").parquet(
        f"{gold}/monthly_ticker_stats/")
    sector_monthly.repartition("year").write.mode("overwrite").partitionBy("year").parquet(
        f"{gold}/sector_monthly_stats/")

    print(
        f"GOLD_SUMMARY silver_rows={silver.count()} ticker_summary_rows={ticker_summary.count()} "
        f"monthly_ticker_rows={monthly.count()} sector_monthly_rows={sector_monthly.count()}"
    )
    spark.stop()


if __name__ == "__main__":
    main()
