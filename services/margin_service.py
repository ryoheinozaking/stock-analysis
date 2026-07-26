# -*- coding: utf-8 -*-
"""JPX「銘柄別信用取引週末残高」PDF の取得・パース・アーカイブ。

PDF テキスト抽出は scripts/extract_pdf.extract_text() に委譲（fitz を直接 import しない）。
"""
from __future__ import annotations

import glob
import os
import re
from typing import Optional

import pandas as pd

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MARGIN_DIR = os.path.join(_ROOT, "data", "margin")

_CODE_RE = re.compile(r"^\d{4}0$")          # 5桁コード（末尾0）
_ISIN_RE = re.compile(r"^JP\d{10}$")
_NUM_RE = re.compile(r"^[▲△+\-]?\s*[\d,]+$")


def _to_int(token: str) -> int:
    t = re.sub(r"\s", "", token)  # 半角/全角スペース除去（"▲ 200" 対策）
    t = t.replace(",", "").replace("▲", "-").replace("△", "-").replace("+", "")
    return int(t)


def parse_margin_text(text: str) -> pd.DataFrame:
    """抽出テキストから銘柄別信用残をパースする。

    各銘柄ブロック: 銘柄名 → コード(5桁) → ISIN → 売残高 → 前週比 →
    買残高 → 前週比 → (一般/制度の内訳が続く)
    Returns 列: code, sell_balance, sell_wow, buy_balance, buy_wow, margin_ratio
    """
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    rows = []
    i = 0
    while i < len(lines):
        if _CODE_RE.match(lines[i]) and i + 1 < len(lines) and _ISIN_RE.match(lines[i + 1]):
            code = lines[i]
            nums = []
            j = i + 2
            while j < len(lines) and len(nums) < 4 and _NUM_RE.match(lines[j]):
                nums.append(_to_int(lines[j]))
                j += 1
            if len(nums) == 4:
                sell, sell_wow, buy, buy_wow = nums
                ratio = (buy / sell) if sell else float("nan")
                rows.append({
                    "code": code, "sell_balance": sell, "sell_wow": sell_wow,
                    "buy_balance": buy, "buy_wow": buy_wow, "margin_ratio": ratio,
                })
            i = j
        else:
            i += 1
    if not rows:
        raise ValueError("信用残データを1件も抽出できませんでした（フォーマット変更の可能性）")
    return pd.DataFrame(rows)


def load_latest_margin() -> Optional[pd.DataFrame]:
    """data/margin/ の最新（日付最大）アーカイブを返す。無ければ None。"""
    if not os.path.isdir(MARGIN_DIR):
        return None
    files = sorted(glob.glob(os.path.join(MARGIN_DIR, "*.parquet")))
    if not files:
        return None
    return pd.read_parquet(files[-1])


def save_margin_archive(df: pd.DataFrame, as_of_yyyymmdd: str) -> str:
    """信用残 DataFrame を data/margin/{YYYYMMDD}.parquet に保存し、パスを返す。"""
    os.makedirs(MARGIN_DIR, exist_ok=True)
    path = os.path.join(MARGIN_DIR, f"{as_of_yyyymmdd}.parquet")
    df.to_parquet(path, index=False)
    return path


def download_margin_pdf(as_of_yyyymmdd: str, dest_path: str) -> str:
    """JPX 週次信用 PDF をダウンロードして dest_path に保存。

    URL 規則: .../margin/tvdivq0000001rnl-att/syumatsu{YYYYMMDD}00.pdf
    ※ 恒久運用は Streamlit「データ更新」から呼ぶ。
    TLS 証明書検証はデフォルト（有効）のまま。OS の証明書ストアで JPX の
    証明書チェーンは検証できることを確認済み（検証を無効化しない）。
    """
    import urllib.request

    url = (
        "https://www.jpx.co.jp/markets/statistics-equities/margin/"
        f"tvdivq0000001rnl-att/syumatsu{as_of_yyyymmdd}00.pdf"
    )
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    # 既定の検証付き SSL コンテキストを使用（context 未指定 = 検証有効）
    with urllib.request.urlopen(req, timeout=60) as r:
        data = r.read()
    with open(dest_path, "wb") as f:
        f.write(data)
    return dest_path
