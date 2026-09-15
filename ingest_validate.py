from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DateType, DoubleType, LongType, StringType
from pyspark.sql.functions import col, count, isnan, when

spark = SparkSession.builder.appName("stock_etl_ingest").master("local[*]").getOrCreate()

# Explicit schema — enforces types instead of letting Spark infer
schema = StructType([
    StructField("Date", DateType(), True),
    StructField("Open", DoubleType(), True),
    StructField("High", DoubleType(), True),
    StructField("Low", DoubleType(), True),
    StructField("Close", DoubleType(), True),
    StructField("Volume", LongType(), True),
    StructField("Ticker", StringType(), True),
])

df = spark.read.csv("./data/raw_stock_data.csv", header=True, schema=schema)

print("Total rows read:", df.count())

# --- Validation ---

# 1. Null check per column
print("\nNull counts per column:")
df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()

# 2. Duplicate (Ticker, Date) check
dupes = df.groupBy("Ticker", "Date").count().filter(col("count") > 1)
dupe_count = dupes.count()
print(f"\nDuplicate (Ticker, Date) pairs: {dupe_count}")
if dupe_count > 0:
    dupes.show()

# 3. Schema mismatch check — rows where Date failed to parse (would show as null after schema read)
bad_dates = df.filter(col("Date").isNull())
print(f"\nRows with unparseable dates: {bad_dates.count()}")

# --- Clean ---
# Drop rows with nulls in critical columns, drop exact duplicates
clean_df = df.dropna(subset=["Date", "Ticker", "Close"]).dropDuplicates(["Ticker", "Date"])

print(f"\nRows after cleaning: {clean_df.count()}")

clean_df.show(5)

# Keep the SparkSession alive for now — we'll reuse it in the next script for transformations