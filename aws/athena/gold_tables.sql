-- Gold-layer tables for Athena. Run silver_table.sql first (it creates the stock_etl database).
-- Run each statement separately in the Athena query editor.
-- If your bucket is not stock-etl-jainil-1101, replace the name in every LOCATION and storage.location.template.

CREATE EXTERNAL TABLE IF NOT EXISTS stock_etl.gold_ticker_summary (
  ticker            string,
  first_trade_date  date,
  last_trade_date   date,
  trading_days      bigint,
  first_close       double,
  last_close        double,
  total_return      double,
  annualized_vol    double,
  max_drawdown      double,
  avg_daily_volume  double,
  best_day_return   double,
  worst_day_return  double
)
STORED AS PARQUET
LOCATION 's3://stock-etl-jainil-1101/gold/ticker_summary/';


CREATE EXTERNAL TABLE IF NOT EXISTS stock_etl.gold_monthly_ticker_stats (
  ticker           string,
  month            int,
  trading_days     bigint,
  monthly_return   double,
  month_end_close  double,
  avg_close        double,
  total_volume     bigint,
  annualized_vol   double
)
PARTITIONED BY (year int)
STORED AS PARQUET
LOCATION 's3://stock-etl-jainil-1101/gold/monthly_ticker_stats/'
TBLPROPERTIES (
  'projection.enabled'         = 'true',
  'projection.year.type'       = 'integer',
  'projection.year.range'      = '2015,2035',
  'storage.location.template'  = 's3://stock-etl-jainil-1101/gold/monthly_ticker_stats/year=${year}'
);


CREATE EXTERNAL TABLE IF NOT EXISTS stock_etl.gold_sector_monthly_stats (
  sector              string,
  month               int,
  tickers             bigint,
  avg_monthly_return  double,
  avg_annualized_vol  double
)
PARTITIONED BY (year int)
STORED AS PARQUET
LOCATION 's3://stock-etl-jainil-1101/gold/sector_monthly_stats/'
TBLPROPERTIES (
  'projection.enabled'         = 'true',
  'projection.year.type'       = 'integer',
  'projection.year.range'      = '2015,2035',
  'storage.location.template'  = 's3://stock-etl-jainil-1101/gold/sector_monthly_stats/year=${year}'
);


-- Sanity checks once the Gold job has run:
-- SELECT * FROM stock_etl.gold_ticker_summary ORDER BY total_return DESC;
-- SELECT sector, year, month, round(avg_monthly_return * 100, 2) AS pct FROM stock_etl.gold_sector_monthly_stats
--   WHERE year = 2026 ORDER BY month, sector;
