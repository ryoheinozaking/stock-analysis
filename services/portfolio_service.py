# -*- coding: utf-8 -*-
"""
SBI証券 ポートフォリオCSV パーサー
"""

import re
import io
import pandas as pd
import streamlit as st
from typing import Tuple


# 株式セクション判定
_EQUITY_SECTIONS = {
    "株式（現物/特定預り）": "特定",
    "株式（現物/NISA預り（成長投資枠））": "NISA成長",
    "株式（現物/NISA預り（つみたて投資枠））": "NISAつみたて",
}
_EQUITY_HEADER = "銘柄（コード）"

_FUND_HEADER = "ファンド名"

_SKIP_PATTERNS = ["合計", "評価額", "ページ", "総件数", "選択範囲", "PTS", "一括", "ポートフォリオ"]


def _is_skip_line(line: str) -> bool:
    return any(p in line for p in _SKIP_PATTERNS)


def _parse_value(s: str) -> float:
    """'+1,234.56' や '-1234' を float に変換"""
    try:
        return float(str(s).replace(",", "").replace("+", "").strip())
    except (ValueError, AttributeError):
        return 0.0


def parse_sbi_csv(file_obj) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    SBI証券ポートフォリオCSVを解析する。

    Returns:
        stocks_df  : 株式保有明細 DataFrame
        summary_df : 口座別合計 DataFrame
    """
    # バイト列なら CP932 でデコード
    if hasattr(file_obj, "read"):
        raw = file_obj.read()
        if isinstance(raw, bytes):
            content = raw.decode("cp932", errors="replace")
        else:
            content = raw
    else:
        content = file_obj

    lines = content.splitlines()

    holdings = []
    funds = []
    current_account = None
    in_equity_section = False
    in_fund_section = False

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # 株式セクション判定
        matched_equity = None
        for section_text, account_label in _EQUITY_SECTIONS.items():
            if section_text in line:
                matched_equity = account_label
                break
        if matched_equity:
            current_account = matched_equity
            in_equity_section = True
            in_fund_section = False
            continue

        # 投資信託セクション判定（"投資信託"を含み"合計"を含まない行）
        if "投資信託" in line and "合計" not in line:
            if "つみたて投資枠" in line:
                current_account = "NISAつみたて"
            elif "旧つみたて" in line:
                current_account = "旧NISAつみたて"
            elif "特定" in line:
                current_account = "特定"
            elif "NISA" in line:
                current_account = "NISA"
            else:
                current_account = "投資信託"
            in_fund_section = True
            in_equity_section = False
            continue

        # ヘッダー行スキップ
        if _EQUITY_HEADER in line:
            continue
        if _FUND_HEADER in line:
            continue

        if _is_skip_line(line):
            continue

        # CSV行をパース
        try:
            row = next(iter(pd.read_csv(
                io.StringIO(line), header=None, dtype=str
            ).values.tolist()), None)
            if row is None or len(row) < 9:
                continue

            if in_equity_section:
                code_name = str(row[0]).strip()
                m = re.match(r"^([A-Z0-9]{4}[A-Z0-9]?)\s+(.+)$", code_name)
                if not m:
                    continue
                code_raw = m.group(1).strip()
                name = m.group(2).strip()
                code_4 = code_raw[:4] if len(code_raw) >= 4 else code_raw
                code_5 = code_4 + "0" if re.match(r"^\d{4}$", code_4) else code_raw

                buy_date = str(row[1]).strip().replace('"', '') or None
                if buy_date == "----/--/--":
                    buy_date = None

                qty = int(_parse_value(row[2]))
                acq_price = _parse_value(row[3])
                cur_price = _parse_value(row[4])
                day_change = _parse_value(row[5])
                day_change_pct = _parse_value(row[6])
                pnl = _parse_value(row[7])
                pnl_pct = _parse_value(row[8])
                market_val = _parse_value(row[9]) if len(row) > 9 else cur_price * qty

                holdings.append({
                    "code_4": code_4, "code_5": code_5, "会社名": name,
                    "口座": current_account, "買付日": buy_date,
                    "数量": qty, "取得単価": acq_price, "現在値": cur_price,
                    "前日比": day_change, "前日比(%)": day_change_pct,
                    "損益": pnl, "損益(%)": pnl_pct, "評価額": market_val,
                    "取得総額": acq_price * qty,
                })

            elif in_fund_section:
                name = str(row[0]).strip()
                # 数字のみの行（合計行）はスキップ
                if not name or name.startswith('"') or re.match(r'^[\d\+\-\.,]+$', name):
                    continue

                buy_date = str(row[1]).strip().replace('"', '') or None
                if buy_date == "----/--/--":
                    buy_date = None

                units = _parse_value(row[2])
                acq_price = _parse_value(row[3])
                cur_price = _parse_value(row[4])
                day_change = _parse_value(row[5])
                day_change_pct = _parse_value(row[6])
                pnl = _parse_value(row[7])
                pnl_pct = _parse_value(row[8])
                market_val = _parse_value(row[9]) if len(row) > 9 else 0.0

                funds.append({
                    "ファンド名": name, "口座": current_account, "受付日": buy_date,
                    "口数": units, "取得単価": acq_price, "基準価額": cur_price,
                    "前日比": day_change, "前日比(%)": day_change_pct,
                    "損益": pnl, "損益(%)": pnl_pct, "評価額": market_val,
                    "取得総額": acq_price * units / 10000,
                })
        except Exception:
            continue

    stocks_df = pd.DataFrame(holdings) if holdings else pd.DataFrame()
    funds_df  = pd.DataFrame(funds)    if funds    else pd.DataFrame()

    return stocks_df, funds_df
