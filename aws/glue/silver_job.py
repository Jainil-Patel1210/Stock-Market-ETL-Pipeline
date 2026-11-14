"""Silver layer: validate, clean and enrich the raw Bronze stock data.

Reads    <bronze_path>/ingest_date=YYYY-MM-DD/*.csv   (every ingest, so windows see full history)
Writes   <silver_path>        Parquet, partitioned by year (clean, de-duplicated, with metrics)
         <quarantine_path>    Parquet of rejected rows with a reject_reason
         <metrics_path>       (optional) one JSON file of run metrics for the data quality gate

The job recomputes Silver from all of Bronze on every run and overwrites the output,
so re-running it is safe (idempotent) and the windowed metrics always see full history.

Runs unchanged as an AWS Glue job or locally:
    python silver_job.py --bronze_path ./bronze --silver_path ./silver --quarantine_path ./quarantine
"""
import argparse

from pyspark.sql import Row, SparkSession, Window

try:
    from pyspark.errors import AnalysisException  # newer PySpark
except ImportError:
    from pyspark.sql.utils import AnalysisException  # older PySpark (Glue 4.0)
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType, DoubleType, LongType, StringType, StructField, StructType,
)

BRONZE_SCHEMA = StructType([
    StructField("Date", DateType(), True),
    StructField("Open", DoubleType(), True),
    StructField("High", DoubleType(), True),
    StructField("Low", DoubleType(), True),
    StructField("Close", DoubleType(), True),
    StructField("Volume", LongType(), True),
    StructField("Ticker", StringType(), True),
])


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze_path", required=True)
    parser.add_argument("--silver_path", required=True)
    parser.add_argument("--quarantine_path", required=True)
    parser.add_argument("--metrics_path", default=None,
                        help="optional: where to write run metrics as JSON for the quality gate")
    # Glue adds its own arguments (--JOB_NAME and others), so ignore unknown ones.
    args, _ = parser.parse_known_args()
    return args


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("stock_etl_silver").getOrCreate()

    # --- Read Bronze. ingest_date is picked up from the folder name (partition discovery).
    raw = (
        spark.read
        .schema(BRONZE_SCHEMA)
        .option("header", True)
        .csv(args.bronze_path)
        .withColumn("source_file", F.col("_metadata.file_path"))  # used only to break ties below
        .withColumnRenamed("Date", "trade_date")
        .withColumnRenamed("Open", "open")
        .withColumnRenamed("High", "high")
        .withColumnRenamed("Low", "low")
        .withColumnRenamed("Close", "close")
        .withColumnRenamed("Volume", "volume")
        .withColumnRenamed("Ticker", "ticker")
        .cache()
    )
    raw_count = raw.count()

    # --- Validate: rejected rows go to quarantine instead of being silently dropped.
    reject_reason = (
        F.when(F.col("trade_date").isNull(), "null_or_unparseable_date")
        .when(F.col("ticker").isNull(), "null_ticker")
        .when(F.col("close").isNull(), "null_close")
        .when(F.col("close") <= 0, "non_positive_close")
        .when(F.col("high") < F.col("low"), "high_below_low")
        .when(F.col("volume") < 0, "negative_volume")
    )
    checked = raw.withColumn("reject_reason", reject_reason)
    rejected = checked.filter(F.col("reject_reason").isNotNull())
    valid = checked.filter(F.col("reject_reason").isNull()).drop("reject_reason")

    # --- De-duplicate on (ticker, trade_date), keeping the most recent ingest.
    # source_file breaks ties between two files in the same ingest_date folder, deterministically.
    latest_first = Window.partitionBy("ticker", "trade_date").orderBy(
        F.col("ingest_date").desc(), F.col("source_file").desc())
    deduped = (
        valid
        .withColumn("_rn", F.row_number().over(latest_first))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "source_file")
    )

    # --- Enrich: per-ticker windowed metrics.
    by_date = Window.partitionBy("ticker").orderBy("trade_date")
    last_7_rows = by_date.rowsBetween(-6, 0)  # current row + 6 preceding trading days

    silver = (
        deduped
        # Only emit a moving average once a full 7-row window exists.
        .withColumn(
            "moving_avg_7d",
            F.when(
                F.count("close").over(last_7_rows) == 7,
                F.round(F.avg("close").over(last_7_rows), 4),
            ),
        )
        .withColumn("prev_close", F.lag("close").over(by_date))
        .withColumn("daily_return", F.round((F.col("close") - F.col("prev_close")) / F.col("prev_close"), 6))
        .drop("prev_close")
        .withColumn("year", F.year("trade_date"))
        .select(
            "ticker", "trade_date", "open", "high", "low", "close", "volume",
            "moving_avg_7d", "daily_return", "ingest_date", "year",
        )
    )

    # --- Remember how big Silver was before this run, so the quality gate can spot a shrink.
    try:
        previous_silver_rows = spark.read.parquet(args.silver_path).count()
    except AnalysisException:  # first run: nothing there yet
        previous_silver_rows = None

    # --- Write. repartition("year") gives one file per year partition instead of many tiny ones.
    (
        silver.repartition("year")
        .write.mode("overwrite")
        .partitionBy("year")
        .parquet(args.silver_path)
    )
    rejected.write.mode("overwrite").parquet(args.quarantine_path)

    # --- Run summary (goes to the job's CloudWatch log).
    rejected_count = rejected.count()
    stats = (
        spark.read.parquet(args.silver_path)
        .agg(
            F.count("*").alias("silver_rows"),
            F.countDistinct("ticker", "trade_date").alias("distinct_keys"),
            F.countDistinct("ticker").alias("ticker_count"),
            F.min("trade_date").cast("string").alias("min_trade_date"),
            F.max("trade_date").cast("string").alias("max_trade_date"),
        )
        .first()
    )
    silver_count = stats["silver_rows"]
    dupes_removed = raw_count - rejected_count - silver_count
    print(f"SILVER_SUMMARY raw_rows={raw_count} rejected_rows={rejected_count} "
          f"duplicates_removed={dupes_removed} silver_rows={silver_count}")
    if rejected_count:
        rejected.groupBy("reject_reason").count().show(truncate=False)

    # --- Publish run metrics for the data quality gate (a single JSON file).
    if args.metrics_path:
        metrics = {
            "raw_rows": raw_count,
            "rejected_rows": rejected_count,
            "duplicates_removed": dupes_removed,
            "silver_rows": silver_count,
            "previous_silver_rows": previous_silver_rows,
            "distinct_keys": stats["distinct_keys"],
            "ticker_count": stats["ticker_count"],
            "min_trade_date": stats["min_trade_date"],
            "max_trade_date": stats["max_trade_date"],
        }
        metrics = Row(**{k: v for k, v in metrics.items() if v is not None})
        spark.createDataFrame([metrics]).coalesce(1).write.mode("overwrite").json(args.metrics_path)

    spark.stop()


if __name__ == "__main__":
    main()
