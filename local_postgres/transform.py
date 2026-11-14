from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DateType, DoubleType, LongType, StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import avg, lag, col, round as spark_round

spark = SparkSession.builder.appName("stock_etl_transform").master("local[*]").getOrCreate()

schema = StructType([
    StructField("Date", DateType(), True),
    StructField("Open", DoubleType(), True),
    StructField("High", DoubleType(), True),
    StructField("Low", DoubleType(), True),
    StructField("Close", DoubleType(), True),
    StructField("Volume", LongType(), True),
    StructField("Ticker", StringType(), True),
])

df = spark.read.csv("data/raw_stock_data.csv", header=True, schema=schema)
df = df.dropna(subset=["Date", "Ticker", "Close"]).dropDuplicates(["Ticker", "Date"])

# --- Windowed transformations ---
# Partition by Ticker so each stock's window is independent, order by Date for time-based logic
window_spec = Window.partitionBy("Ticker").orderBy("Date")

# 7-day moving average of Close price
moving_avg_window = window_spec.rowsBetween(-6, 0)  # current row + 6 preceding = 7-day window
df = df.withColumn("moving_avg_7d", spark_round(avg("Close").over(moving_avg_window), 4))

# Daily return: (today's close - yesterday's close) / yesterday's close
df = df.withColumn("prev_close", lag("Close").over(window_spec))
df = df.withColumn(
    "daily_return",
    spark_round((col("Close") - col("prev_close")) / col("prev_close"), 6)
)

df = df.drop("prev_close")  # was only needed as an intermediate step

print("Sample output for AAPL:")
df.filter(col("Ticker") == "AAPL").orderBy("Date").show(10)

# Save transformed data for the load step (we'll load this into Postgres next)
df.toPandas().to_csv("./data/transformed_stock_data.csv", index=False)
print("Transformed data saved to data/transformed_stock_data.csv")