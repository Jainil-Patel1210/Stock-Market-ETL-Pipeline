-- Star-schema DDL for the local PostgreSQL version of the pipeline (load.py).
--
-- NOTE: reconstructed from the INSERT / ON CONFLICT statements in load.py, because the
-- original DDL was never committed. If your database was created with extra columns
-- (for example company_name or sector on dim_stocks), compare with `\d dim_stocks` in psql.
--
-- Usage:  psql -U postgres -d stock_etl -f local_postgres/schema.sql

CREATE TABLE IF NOT EXISTS dim_stocks (
    ticker_id  SERIAL PRIMARY KEY,
    ticker     VARCHAR(10) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_dates (
    date_id  SERIAL PRIMARY KEY,
    date     DATE NOT NULL UNIQUE,
    year     INT  NOT NULL,
    month    INT  NOT NULL,
    day      INT  NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_daily_prices (
    ticker_id     INT NOT NULL REFERENCES dim_stocks (ticker_id),
    date_id       INT NOT NULL REFERENCES dim_dates (date_id),
    open          DOUBLE PRECISION,
    high          DOUBLE PRECISION,
    low           DOUBLE PRECISION,
    close         DOUBLE PRECISION,
    volume        BIGINT,
    moving_avg    DOUBLE PRECISION,
    daily_return  DOUBLE PRECISION,
    -- load.py upserts with ON CONFLICT (ticker_id, date_id), which needs this unique key
    PRIMARY KEY (ticker_id, date_id)
);
