# -*- coding: utf-8 -*-
"""
prices.parquet の歴史データを遡及取得するワンタイムスクリプト。
既存データ（2025-11-25〜）の前に 2023-01-01〜2025-11-24 を取得して結合する。
"""

import os, sys, time
import pandas as pd
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

from services.batch_service import _get, PRICES_PATH

FROM_DATE = "2021-01-01"
TO_DATE   = "2022-12-31"   # 既存データ（2023-01〜）の前日まで

def main():
    print(f"歴史データ取得: {FROM_DATE} 〜 {TO_DATE}")

    # 取得日リスト（平日のみ）
    date_strs = [
        d.strftime("%Y-%m-%d")
        for d in pd.date_range(FROM_DATE, TO_DATE, freq="B")
    ]
    total = len(date_strs)
    print(f"取得予定: {total} 営業日")

    new_frames = []
    for i, date_str in enumerate(date_strs):
        if i % 50 == 0:
            print(f"  {i}/{total} ({date_str})", flush=True)
        try:
            data = _get("/equities/bars/daily", {"date": date_str})
            df   = pd.DataFrame(data.get("data", []))
            if not df.empty:
                new_frames.append(df)
        except Exception as e:
            print(f"  skip {date_str}: {e}", flush=True)
        time.sleep(0.1)  # rate limit対策

    if not new_frames:
        print("取得データなし。終了。")
        return

    hist = pd.concat(new_frames, ignore_index=True)
    print(f"取得完了: {len(hist):,} 行")

    # 既存データと結合
    if os.path.exists(PRICES_PATH):
        existing = pd.read_parquet(PRICES_PATH)
        print(f"既存データ: {len(existing):,} 行")
        combined = pd.concat([hist, existing], ignore_index=True)
    else:
        combined = hist

    combined = combined.drop_duplicates(subset=["Date", "Code"]).reset_index(drop=True)
    combined = combined.sort_values(["Code", "Date"]).reset_index(drop=True)
    combined.to_parquet(PRICES_PATH, index=False)
    print(f"保存完了: {len(combined):,} 行")
    print(f"期間: {combined['Date'].min()} 〜 {combined['Date'].max()}")

if __name__ == "__main__":
    main()
