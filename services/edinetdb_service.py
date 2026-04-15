# -*- coding: utf-8 -*-
"""
EDINET DB API サービス
https://edinetdb.jp/v1

認証: X-API-Key ヘッダー
無料プラン: 100 req/日
キャッシュ: data/edinetdb_cache/ に保存してリクエスト節約
"""

import json
import os
import time
from datetime import datetime, timedelta
from typing import Optional

import requests
from dotenv import load_dotenv

load_dotenv('.env')

BASE_URL = "https://edinetdb.jp/v1"
CACHE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "edinetdb_cache")


def _api_key() -> str:
    return os.getenv("EDINETDB_API_KEY", "")


def _headers() -> dict:
    return {"X-API-Key": _api_key()}


def _cache_path(edinet_code: str, endpoint: str) -> str:
    os.makedirs(CACHE_DIR, exist_ok=True)
    safe = endpoint.replace("/", "_").strip("_")
    return os.path.join(CACHE_DIR, f"{edinet_code}_{safe}.json")


def _load_cache(path: str, ttl_hours: int = 24) -> Optional[dict]:
    if not os.path.exists(path):
        return None
    age = time.time() - os.path.getmtime(path)
    if age > ttl_hours * 3600:
        return None
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _save_cache(path: str, data: dict) -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def _get(endpoint: str, params: Optional[dict] = None) -> dict:
    url = f"{BASE_URL}{endpoint}"
    resp = requests.get(url, headers=_headers(), params=params or {}, timeout=30)
    resp.raise_for_status()
    return resp.json()


# ---- 公開API ----

def search_company(query: str) -> Optional[dict]:
    """
    銘柄コードまたは会社名でEDINETコードを解決する。
    返り値: {edinet_code, name, sec_code, ...} または None
    """
    try:
        data = _get("/search", {"q": query.strip(), "limit": 1})
        companies = data.get("data", [])
        return companies[0] if companies else None
    except Exception as e:
        raise RuntimeError(f"企業検索に失敗しました: {e}")


def get_company_info(edinet_code: str) -> dict:
    """
    企業基本情報 + 最新財務サマリー + 財務健全性スコアを取得。
    キャッシュTTL: 24時間
    """
    cache_path = _cache_path(edinet_code, "info")
    cached = _load_cache(cache_path, ttl_hours=24)
    if cached:
        return cached

    data = _get(f"/companies/{edinet_code}")
    result = data.get("data", data)
    _save_cache(cache_path, result)
    return result


def get_financials(edinet_code: str, years: int = 5) -> dict:
    """
    財務時系列データ（損益計算書・貸借対照表・CF計算書）を取得。
    years: 最大6期
    キャッシュTTL: 24時間
    """
    cache_path = _cache_path(edinet_code, f"financials_{years}y")
    cached = _load_cache(cache_path, ttl_hours=24)
    if cached:
        return cached

    data = _get(f"/companies/{edinet_code}/financials", {"years": years, "period": "annual"})
    result = data.get("data", data)
    _save_cache(cache_path, result)
    return result


def get_shareholders(edinet_code: str) -> list:
    """
    大量保有報告書（5%以上の大株主）を取得。
    キャッシュTTL: 24時間
    """
    cache_path = _cache_path(edinet_code, "shareholders")
    cached = _load_cache(cache_path, ttl_hours=24)
    if cached is not None:
        return cached

    data = _get(f"/companies/{edinet_code}/shareholders")
    result = data.get("data", data)
    if isinstance(result, dict):
        result = result.get("shareholders", result.get("items", []))
    _save_cache(cache_path, result)
    return result


def get_text_blocks(edinet_code: str, fiscal_year: Optional[int] = None) -> dict:
    """
    有価証券報告書の主要4セクションのテキストを取得。
    - 事業の状況 / 事業等のリスク / 経営者による分析(MD&A) / 経営方針
    キャッシュTTL: 7日（有報は提出後変更なし）
    """
    fy_suffix = f"_fy{fiscal_year}" if fiscal_year else ""
    cache_path = _cache_path(edinet_code, f"text_blocks{fy_suffix}")
    cached = _load_cache(cache_path, ttl_hours=24 * 7)
    if cached:
        return cached

    params = {}
    if fiscal_year:
        params["fiscal_year"] = fiscal_year
    data = _get(f"/companies/{edinet_code}/text-blocks", params)
    result = data.get("data", data)
    _save_cache(cache_path, result)
    return result


def get_earnings(edinet_code: str, limit: int = 8) -> list:
    """
    TDNet決算短信データを取得（直近N件）。
    キャッシュTTL: 6時間
    """
    cache_path = _cache_path(edinet_code, f"earnings_{limit}")
    cached = _load_cache(cache_path, ttl_hours=6)
    if cached is not None:
        return cached

    data = _get(f"/companies/{edinet_code}/earnings", {"limit": limit})
    inner = data.get("data", data)
    result = inner.get("earnings", inner) if isinstance(inner, dict) else inner
    _save_cache(cache_path, result)
    return result


def fetch_all(edinet_code: str) -> dict:
    """
    深層分析に必要な全データを一括取得（キャッシュ活用）。
    返り値: {info, financials, shareholders, text_blocks}
    """
    info = get_company_info(edinet_code)
    financials = get_financials(edinet_code, years=5)
    shareholders = get_shareholders(edinet_code)
    text_blocks = get_text_blocks(edinet_code)

    return {
        "info": info,
        "financials": financials,
        "shareholders": shareholders,
        "text_blocks": text_blocks,
    }
