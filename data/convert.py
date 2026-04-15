import pandas as pd

df = pd.read_parquet("stock_cache.parquet")

# カラム選択（あなたのデータに合わせた）
columns = [
    "code",
    "company_name",
    "close",
    "PER",
    "PBR",
    "ROE",
    "rev_growth",
    "profit_growth",
    "score",
    "signal_score",
    "RSI",
    "avg_volume",
    "latest_volume",
    "mom_signal",
    "mom_new_high",
    "mom_macd",
    "mom_above_ma200"
]

df = df[columns]

# スコア順で上位200銘柄
df = df.sort_values("score", ascending=False).head(200)

df.to_csv("stock_cache_for_claude.csv", index=False)

print("Claude用CSV作成完了")