# -*- coding: utf-8 -*-
"""
一括ダウンロード（data/bulk/）を parquet に取り込む（API コールなし。Claude Code のシェルからも実行可）

使い方:
  .venv\\Scripts\\python.exe scripts\\import_bulk.py [--data-dir DIR]

- 財務情報        → data/fins_cache.parquet に統合（書き込み前にバックアップを作成）
- バリュエーション指標 → data/valuation.parquet に統合
- 決算発表予定日    → data/earnings_dates.parquet に保存
先に scripts/jquants_bulk.py でダウンロードしておくこと。
Streamlit の「データ更新」と同時に実行しないこと（fins_cache を同時に書き換えるため）。
"""
import argparse
import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)

from services.bulk_import import import_earnings_dates, import_fins, import_valuation


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    ap = argparse.ArgumentParser(description="data/bulk/ の一括ダウンロードを parquet に取り込む")
    ap.add_argument("--data-dir", default=os.path.join(_ROOT, "data"),
                    help="data ディレクトリ（bulk/ と parquet の置き場所）")
    args = ap.parse_args()
    data_dir = args.data_dir
    bulk_root = os.path.join(data_dir, "bulk")

    _, stats = import_fins(os.path.join(data_dir, "fins_cache.parquet"), root=bulk_root)
    print("財務情報:", stats)

    val = import_valuation(os.path.join(data_dir, "valuation.parquet"), root=bulk_root)
    print(f"バリュエーション指標: {len(val):,} 行 / {val['Date'].min()} 〜 {val['Date'].max()} / {val['Code'].nunique():,} 銘柄")

    ed = import_earnings_dates(os.path.join(data_dir, "earnings_dates.parquet"), root=bulk_root)
    print(f"決算発表予定日: {len(ed):,} 行 / 公表日 {ed['PubDate'].min()} 〜 {ed['PubDate'].max()}")


if __name__ == "__main__":
    main()
