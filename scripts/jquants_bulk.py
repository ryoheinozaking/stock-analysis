# -*- coding: utf-8 -*-
"""
J-Quants 一括ダウンロード（Light プラン以上）: 月次 gzip CSV を data/bulk/ に同期する

使い方（利用者のターミナルで実行する。Claude Code のシェルからは J-Quants API に届かない）:
  .venv\\Scripts\\python.exe scripts\\jquants_bulk.py
  .venv\\Scripts\\python.exe scripts\\jquants_bulk.py --endpoint /equities/valuation --from 2021-09 --to 2026-09

既定の対象: /equities/valuation, /fins/summary, /fins/earnings-date（期間省略時は契約で取れる全期間）
2 回目以降は、前回から更新されたファイル（訂正の上書きを含む）だけを取り直す。
parquet への取り込みは別工程（services/bulk_service.py の冒頭を参照）。
"""
import argparse
import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)

from dotenv import load_dotenv

load_dotenv(os.path.join(_ROOT, ".env"))

from services.bulk_service import BULK_DIR, sync_bulk

DEFAULT_ENDPOINTS = ["/equities/valuation", "/fins/summary", "/fins/earnings-date"]


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    ap = argparse.ArgumentParser(description="J-Quants の一括ダウンロードファイルを data/bulk/ に同期する")
    ap.add_argument("--endpoint", action="append",
                    help="対象データ（複数指定可）。省略時は " + ", ".join(DEFAULT_ENDPOINTS))
    ap.add_argument("--from", dest="date_from", help="期間の開始（YYYY-MM）。省略時は契約で取れる最古から")
    ap.add_argument("--to", dest="date_to", help="期間の終了（YYYY-MM）。省略時は最新まで")
    args = ap.parse_args()

    endpoints = args.endpoint or DEFAULT_ENDPOINTS
    total_failed = 0
    for endpoint in endpoints:
        print(f"== {endpoint} ==")
        res = sync_bulk(endpoint, args.date_from, args.date_to,
                        progress=lambda i, n, key: print(f"  [{i + 1}/{n}] {key}", flush=True))
        print(f"  取得 {len(res['downloaded'])} 件 / 変更なしで省略 {len(res['skipped'])} 件"
              f" / 失敗 {len(res['failed'])} 件")
        for key, reason in res["failed"]:
            print(f"  失敗: {key}: {reason}")
        total_failed += len(res["failed"])

    print(f"\n保存先: {BULK_DIR}")
    if total_failed:
        print("失敗したファイルがあります。もう一度実行すると、失敗分と未取得分だけを取り直します。")
    else:
        print("完了しました。取り込みは Claude Code に依頼してください。")


if __name__ == "__main__":
    main()
