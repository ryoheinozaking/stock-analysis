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
