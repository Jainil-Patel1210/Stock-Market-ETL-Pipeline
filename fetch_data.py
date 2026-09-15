import yfinance as yf
import pandas as pd

tickers = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META",
    "TSLA", "NVDA", "JPM", "V", "WMT",
    "JNJ", "PG", "UNH", "HD", "DIS",
    "KO", "PEP", "XOM", "CVX", "BA"
]

data = yf.download(tickers, start="2019-01-01", end="2026-09-01", group_by="ticker")

# Reshape wide -> long
long_data = []
for ticker in tickers:
    df = data[ticker].copy()
    df["Ticker"] = ticker
    df = df.reset_index()
    long_data.append(df)

result = pd.concat(long_data, ignore_index=True)
result.to_csv("./data/raw_stock_data.csv", index=False)
print("Saved. Shape:", result.shape)
print(result.head())