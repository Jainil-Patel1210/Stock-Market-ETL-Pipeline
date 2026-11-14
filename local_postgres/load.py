from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DateType, DoubleType, LongType, StringType
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()


# Load transformed data
df = pd.read_csv("data/transformed_stock_data.csv", parse_dates=["Date"])

conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)
cur = conn.cursor()

# --- 1. Upsert dim_stocks ---
tickers = df["Ticker"].unique().tolist()
for t in tickers:
    cur.execute("""
        INSERT INTO dim_stocks (ticker) VALUES (%s)
        ON CONFLICT (ticker) DO NOTHING
    """, (t,))
conn.commit()

# --- 2. Upsert dim_dates ---
dates = df["Date"].dt.date.unique().tolist()
for d in dates:
    cur.execute("""
        INSERT INTO dim_dates (date, year, month, day)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (date) DO NOTHING
    """, (d, d.year, d.month, d.day))
conn.commit()

# --- 3. Build ticker_id and date_id lookup maps ---
cur.execute("SELECT ticker_id, ticker FROM dim_stocks")
ticker_map = {ticker: tid for tid, ticker in cur.fetchall()}

cur.execute("SELECT date_id, date FROM dim_dates")
date_map = {d: did for did, d in cur.fetchall()}

# --- 4. Upsert fact_daily_prices ---
rows = []
for _, row in df.iterrows():
    ticker_id = ticker_map[row["Ticker"]]
    date_id = date_map[row["Date"].date()]
    rows.append((
        ticker_id, date_id, row["Open"], row["High"], row["Low"], row["Close"],
        int(row["Volume"]), row["moving_avg_7d"],
        None if pd.isna(row["daily_return"]) else row["daily_return"]
    ))

execute_values(cur, """
    INSERT INTO fact_daily_prices
        (ticker_id, date_id, open, high, low, close, volume, moving_avg, daily_return)
    VALUES %s
    ON CONFLICT (ticker_id, date_id) DO UPDATE SET
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume = EXCLUDED.volume,
        moving_avg = EXCLUDED.moving_avg,
        daily_return = EXCLUDED.daily_return
""", rows)

conn.commit()
print(f"Loaded {len(rows)} rows into fact_daily_prices.")

cur.close()
conn.close()