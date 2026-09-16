# -*- coding: utf-8 -*-
"""
一括ダウンロード（data/bulk/）の取り込み: gzip CSV → parquet（API コールなし）

実ファイルで確認した事実（2026-09-16、216 ファイル）:
- CSV の列名は API のレスポンス項目名と同一（fins_cache.parquet と列集合が一致）。空欄は空文字
- 過去分は historical/YYYY/<name>_YYYYMM.csv.gz（月次）、当月分は live/<name>_YYYYMMDD.csv.gz（日次）。
  月が変わると月次にまとめ直されるため、同じ行が両方に入りうる → 新しいファイルの行を優先して重複除去
- 財務情報は訂正が上書きで反映される → 一括ダウンロード側を正として fins_cache を更新する
- 一括ダウンロードの財務情報は 2026-03 以前で fins_cache より約 5% 多い（初回の銘柄別全件取得は
  取得時点の上場銘柄だけが対象だったため、その後に上場廃止した銘柄の開示が欠けていたとみられる）
"""
import glob
import os
import re
import shutil
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd

from services.bulk_service import BULK_DIR

_DATE_IN_NAME = re.compile(r"_(\d{6}|\d{8})\.csv\.gz$")

VALUATION_NUMERIC = ["EPS", "FwdEPS", "BPS", "ROE", "FwdROE", "PER", "FwdPER", "PBR", "MktCap"]


def bulk_files(endpoint: str, root: Optional[str] = None) -> List[str]:
    """endpoint（例: 'fins/summary'）のファイルを古い順に返す。

    月次ファイル YYYYMM は YYYYMM00 として並べ、同じ月の日次ファイル YYYYMMDD より前に置く。
    """
    root = root or BULK_DIR
    paths = glob.glob(os.path.join(root, *endpoint.strip("/").split("/"), "**", "*.csv.gz"), recursive=True)

    def _order(path: str) -> str:
        m = _DATE_IN_NAME.search(os.path.basename(path))
        digits = m.group(1) if m else ""
        return digits + "00" if len(digits) == 6 else digits

    return sorted(paths, key=_order)


def read_bulk(endpoint: str, root: Optional[str] = None) -> pd.DataFrame:
    """endpoint の全ファイルを文字列のまま連結する（古いファイルが先、新しいファイルが後）。"""
    frames = [pd.read_csv(p, dtype=str, keep_default_na=False) for p in bulk_files(endpoint, root)]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _backup(path: str) -> Optional[str]:
    if not os.path.exists(path):
        return None
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = path.replace(".parquet", f".backup_{stamp}.parquet")
    shutil.copy2(path, backup_path)
    return backup_path


def import_fins(dest_path: str, root: Optional[str] = None,
                backup: bool = True) -> Tuple[pd.DataFrame, Dict[str, object]]:
    """一括ダウンロードの財務情報を fins_cache に統合する（DiscNo 単位で一括ダウンロード側を優先）。

    戻り値の統計: bulk_rows / cache_rows / added（新規）/ updated（値が変わった）/
                  cache_only（手元にしかない行。一括ダウンロードの期間外など）/ backup
    """
    bulk = read_bulk("fins/summary", root).drop_duplicates("DiscNo", keep="last")
    cache = pd.read_parquet(dest_path) if os.path.exists(dest_path) else pd.DataFrame(columns=bulk.columns)

    common = [c for c in bulk.columns if c in cache.columns and c != "DiscNo"]
    both = cache[["DiscNo"] + common].merge(bulk[["DiscNo"] + common], on="DiscNo", suffixes=("_c", "_b"))
    changed = pd.Series(False, index=both.index)
    for col in common:
        changed |= both[f"{col}_c"].fillna("").astype(str) != both[f"{col}_b"].fillna("").astype(str)

    columns = list(cache.columns) + [c for c in bulk.columns if c not in cache.columns]
    result = (pd.concat([cache, bulk], ignore_index=True)
                .drop_duplicates("DiscNo", keep="last")
                .reindex(columns=columns)
                .fillna("")
                .reset_index(drop=True))

    stats = {
        "bulk_rows": len(bulk), "cache_rows": len(cache),
        "added": int((~bulk["DiscNo"].isin(cache["DiscNo"])).sum()),
        "updated": int(changed.sum()),
        "cache_only": int((~cache["DiscNo"].isin(bulk["DiscNo"])).sum()),
        "backup": _backup(dest_path) if backup else None,
    }
    result.to_parquet(dest_path, index=False)
    return result, stats


def import_valuation(dest_path: str, root: Optional[str] = None) -> pd.DataFrame:
    """一括ダウンロードのバリュエーション指標を valuation.parquet に統合する（(Date, Code) で新しい行を優先）。"""
    bulk = read_bulk("equities/valuation", root)
    existing = pd.read_parquet(dest_path) if os.path.exists(dest_path) else pd.DataFrame()
    frames = [f for f in (existing, bulk) if not f.empty]
    if not frames:
        return pd.DataFrame()
    result = pd.concat(frames, ignore_index=True)
    result["Date"] = result["Date"].astype(str)
    result["Code"] = result["Code"].astype(str)
    for col in VALUATION_NUMERIC:
        if col in result.columns:
            result[col] = pd.to_numeric(result[col].replace("", None), errors="coerce")
    result = (result.drop_duplicates(["Date", "Code"], keep="last")
                    .sort_values(["Date", "Code"])
                    .reset_index(drop=True))
    result.to_parquet(dest_path, index=False)
    return result


def import_earnings_dates(dest_path: str, root: Optional[str] = None) -> pd.DataFrame:
    """決算発表予定日を保存する。予定変更は削除されず新しい行として追加される仕様なので、完全一致の重複だけ落とす。"""
    bulk = read_bulk("fins/earnings-date", root)
    if bulk.empty:
        return bulk
    result = (bulk.drop_duplicates(keep="last")
                  .sort_values(["PubDate", "Code", "FQName"])
                  .reset_index(drop=True))
    result.to_parquet(dest_path, index=False)
    return result
