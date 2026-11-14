-- Silver-layer table for Athena. Run each statement separately in the Athena query editor.
-- If your bucket is not stock-etl-jainil-1101, replace the name in LOCATION and storage.location.template.
-- Before the first query, set Athena's query result location (Settings tab) to s3://<your-bucket>/athena-results/

CREATE DATABASE IF NOT EXISTS stock_etl;


CREATE EXTERNAL TABLE IF NOT EXISTS stock_etl.silver_stocks (
  ticker         string,
  trade_date     date,
  open           double,
  high           double,
  low            double,
  close          double,
  volume         bigint,
  moving_avg_7d  double,
  daily_return   double,
  ingest_date    date
)
PARTITIONED BY (year int)
STORED AS PARQUET
LOCATION 's3://stock-etl-jainil-1101/silver/stocks/'
TBLPROPERTIES (
  'projection.enabled'          = 'true',
  'projection.year.type'        = 'integer',
  'projection.year.range'       = '2015,2035',
  'storage.location.template'   = 's3://stock-etl-jainil-1101/silver/stocks/year=${year}'
);

-- Partition projection tells Athena which year= folders exist, so no MSCK REPAIR TABLE is ever needed.


-- Sanity checks:
-- SELECT count(*) FROM stock_etl.silver_stocks;
--
-- Biggest movers on the latest trading day:
-- SELECT ticker, trade_date, close, round(daily_return * 100, 2) AS pct_change
-- FROM stock_etl.silver_stocks
-- WHERE trade_date = (SELECT max(trade_date) FROM stock_etl.silver_stocks)
-- ORDER BY daily_return DESC
-- LIMIT 5;
--
-- Annualized volatility per ticker, sigma_daily * sqrt(252):
-- SELECT ticker, round(stddev_samp(daily_return) * sqrt(252), 4) AS annualized_vol
-- FROM stock_etl.silver_stocks
-- WHERE year = 2025
-- GROUP BY ticker
-- ORDER BY annualized_vol DESC;
