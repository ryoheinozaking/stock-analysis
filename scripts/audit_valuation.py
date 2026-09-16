# -*- coding: utf-8 -*-
"""
バリュエーション指標の答え合わせ: 自前計算（stock_cache / パイプライン）vs J-Quants 公式の算出値

使い方:
  .venv/Scripts/python.exe scripts/audit_valuation.py [--date YYYY-MM-DD] [--top 15]

前提: Streamlit の「データ更新」で data/valuation.parquet と stock_cache.parquet が最新であること
出力: 標準出力に指標ごとの集計と「要調査」のずれが大きい銘柄、
      data/valuation_audit/audit_YYYYMMDD.csv に全銘柄×全指標の比較結果
分類の意味は services/valuation_audit.py の冒頭を参照。
"""
import argparse
import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)

import numpy as np
import pandas as pd

from services.batch_service import CACHE_PATH, FINS_PATH, PRICES_PATH, VALUATION_PATH
from services.pipeline_service import _build_fins_metrics, _load_fins_fy
from services.valuation_audit import (
    CAT_UNKNOWN, audit_valuation, build_reference_ratios, recent_split_factors, summarize,
)


def main():
    # 出力をパイプに流したときに Windows 既定の文字コードで化けないよう UTF-8 に固定
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    ap = argparse.ArgumentParser(description="自前のバリュエーション指標を J-Quants の算出値と突き合わせる")
    ap.add_argument("--date", help="JPX 側の評価日（省略時は valuation.parquet の最新日）")
    ap.add_argument("--top", type=int, default=15, help="指標ごとに表示する要調査銘柄の件数")
    args = ap.parse_args()

    if not os.path.exists(VALUATION_PATH):
        sys.exit("data/valuation.parquet がありません。Streamlit の「データ更新」で取得してください。")

    val = pd.read_parquet(VALUATION_PATH)
    val_dates = pd.to_datetime(val["Date"])
    date = pd.Timestamp(args.date) if args.date else val_dates.max()
    jpx = val[val_dates == date]
    if jpx.empty:
        sys.exit(f"{date.date()} のバリュエーション指標がありません。")

    stock = pd.read_parquet(CACHE_PATH)
    prices = pd.read_parquet(PRICES_PATH, columns=["Date", "Code", "AdjFactor"])
    price_date = pd.to_datetime(prices["Date"]).max()

    # 自前の時価総額はパイプラインと同じ算出（close × 本決算の ShOutFY を分割調整）
    fins_metrics = _build_fins_metrics(_load_fins_fy(), prices)
    ours = stock.merge(fins_metrics[["code", "sh_out"]], on="code", how="left")
    ours["market_cap"] = ours["close"] * ours["sh_out"]

    fins = pd.read_parquet(FINS_PATH)
    refs = build_reference_ratios(fins, as_of=date)
    splits = recent_split_factors(prices, as_of=date)

    audit = audit_valuation(ours, jpx, refs, splits)
    summary = summarize(audit)

    pd.set_option("display.width", 220)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.unicode.east_asian_width", True)

    print("=== バリュエーション指標の答え合わせ（自前 / JPX）===")
    print(f"JPX の評価日: {date.date()}　自前（stock_cache）の株価日: {price_date.date()}")
    if price_date.normalize() != date.normalize():
        print("注意: 評価日がずれています。株価の違いでも差が出るので --date で同じ日にそろえてください。")
    print(summary.to_string(index=False))

    for metric in summary["metric"]:
        g = audit[(audit["metric"] == metric) & (audit["category"] == CAT_UNKNOWN)].copy()
        if g.empty:
            continue
        n_all = len(g)
        g["_dev"] = np.abs(np.log(g["ratio"].where(g["ratio"] > 0)))
        g = g.sort_values("_dev", ascending=False).head(args.top)
        print(f"\n--- {metric}: 要調査のずれが大きい銘柄（上位 {len(g)} 件 / 全 {n_all} 件）---")
        print(g[["code", "company_name", "ours", "jpx", "ratio", "last_disc_date"]]
              .to_string(index=False, float_format=lambda v: f"{v:,.3f}"))

    out_dir = os.path.join(_ROOT, "data", "valuation_audit")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"audit_{date:%Y%m%d}.csv")
    audit.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"\n全件の比較結果: {out_path}")


if __name__ == "__main__":
    main()
