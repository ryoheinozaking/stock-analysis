# -*- coding: utf-8 -*-
"""
J-Quants 一括ダウンロード（/bulk/list・/bulk/get、Light プラン以上）

月次の gzip CSV を data/bulk/<Key> にそのまま保存する。parquet への取り込みは別工程
（CSV の列名が API の項目名と同じかは仕様書に記載がないため、実ファイルを見てから実装する）。

- /bulk/list でファイル一覧（Key / LastModified / Size）を取得する
- /bulk/get で署名付き URL を取得し、すぐにダウンロードする（URL は 5 分で失効・再利用不可）
- 前回と LastModified・Size が同じファイルは取り直さない。J-Quants は訂正を既存データの
  上書きで反映し差分を提供しないため、定期的に同期し直すと更新されたファイルだけが入れ替わる
  （公式の「必要な範囲を定期的に再取得」の推奨に沿う）

API は Claude Code のシェルから届かない（SSL 制限）ため、利用者のターミナルで
scripts/jquants_bulk.py から実行する。
"""
import json
import os
import time
from typing import Callable, Dict, List, Optional

import requests

from services.batch_service import _get, _get_all

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BULK_DIR = os.path.join(_ROOT, "data", "bulk")
MANIFEST_NAME = "manifest.json"
API_INTERVAL_SEC = 1.1   # 一括ダウンロード系のレート制限は非公開のため 1 秒 1 回に抑える（Light は 60 回/分）


def list_files(endpoint: str, date_from: Optional[str] = None,
               date_to: Optional[str] = None) -> List[dict]:
    """ダウンロード可能なファイル一覧。期間を省略すると契約プランで取れる全期間。"""
    params = {"endpoint": endpoint}
    if date_from:
        params["from"] = date_from
    if date_to:
        params["to"] = date_to
    return _get_all("/bulk/list", params)


def _safe_path(root: str, key: str) -> str:
    """Key を root 配下のパスにする。root の外に出るキーは拒否する。"""
    root_n = os.path.normpath(root)
    path = os.path.normpath(os.path.join(root_n, key))
    if not path.startswith(root_n + os.sep):
        raise ValueError(f"不正なファイルキーです: {key}")
    return path


def _load_manifest(root: str) -> Dict[str, dict]:
    try:
        with open(os.path.join(root, MANIFEST_NAME), encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def _save_manifest(root: str, manifest: Dict[str, dict]) -> None:
    os.makedirs(root, exist_ok=True)
    with open(os.path.join(root, MANIFEST_NAME), "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)


def download_file(key: str, dest_path: str, timeout: int = 120) -> int:
    """署名付き URL を取得してすぐダウンロードし、保存したバイト数を返す。"""
    url = _get("/bulk/get", {"key": key})["url"]
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    tmp_path = dest_path + ".part"
    with requests.get(url, stream=True, timeout=timeout) as r:
        r.raise_for_status()
        with open(tmp_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                f.write(chunk)
    os.replace(tmp_path, dest_path)
    return os.path.getsize(dest_path)


def sync_bulk(endpoint: str, date_from: Optional[str] = None, date_to: Optional[str] = None,
              root: Optional[str] = None,
              progress: Optional[Callable[[int, int, str], None]] = None,
              sleep: Callable[[float], None] = time.sleep) -> Dict[str, list]:
    """endpoint の月次ファイルを root に同期する。

    戻り値: {"downloaded": [key...], "skipped": [key...], "failed": [(key, 理由)...]}
    マニフェストは 1 ファイルごとに保存するので、途中で止めても次回は続きから取得する。
    """
    root = root or BULK_DIR
    files = list_files(endpoint, date_from, date_to)
    manifest = _load_manifest(root)
    result = {"downloaded": [], "skipped": [], "failed": []}

    for i, meta in enumerate(files):
        key = meta["Key"]
        try:
            path = _safe_path(root, key)
            prev = manifest.get(key)
            if (prev and prev.get("LastModified") == meta.get("LastModified")
                    and os.path.exists(path) and os.path.getsize(path) == meta.get("Size")):
                result["skipped"].append(key)
                continue
            if progress:
                progress(i, len(files), key)
            sleep(API_INTERVAL_SEC)
            size = download_file(key, path)
            if meta.get("Size") is not None and size != meta["Size"]:
                os.remove(path)
                raise IOError(f"サイズ不一致（取得 {size} / 一覧 {meta['Size']}）")
        except Exception as e:
            result["failed"].append((key, str(e)))
            continue
        manifest[key] = {"LastModified": meta.get("LastModified"), "Size": size}
        _save_manifest(root, manifest)
        result["downloaded"].append(key)

    return result
