# -*- coding: utf-8 -*-
"""
経営変化スコア（Governance Score）

【背景】 ChatGPT-5 提案の「経営変化スコア」フレームワーク + 戦略の核心思想
  「PBR 改善 = 利益改善 × 経営の意志」
における「経営の意志」を数値化することで、ディープバリュー戦略の真の α 源
（PBR 改善カタリスト）を捕捉する。

【現状（2026-04-30）】 EDINET DB MCP の API 制限・カバレッジ制約により MVP 実装。
- ✅ アクティビスト保有検出（バルク取得済・カバレッジ良好）
- ⏳ PBR 改善開示（次フェーズ：search_ir_sections の 50件上限を回避する設計が必要）
- ⏳ 政策保有株削減（次フェーズ：edinet_code マッピング必要）
- ⏳ 自社株買い検出（次フェーズ：get_events で時系列収集）

【スコア（最大10pt の MVP 版）】
- アクティビスト保有あり: +10pt（旧村上ファンド、ストラテジックキャピタル、
  3D Investment Partners、シルチェスター、エフィッシモ等が保有）

【データキャッシュ】 data/governance_activists.json
- 月次でリフレッシュ推奨（手動）
- 内容: sec_code (4桁) → アクティビスト保有情報リスト
"""
import os
import json
from typing import Optional

import pandas as pd
import numpy as np


_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ACTIVIST_CACHE_PATH = os.path.join(_ROOT, "data", "governance_activists.json")


def _load_activist_cache() -> dict:
    """アクティビスト保有データをキャッシュから読み込む。
    なければ空辞書を返す（governance スコアが 0 になるだけで動作には影響しない）。"""
    if not os.path.exists(ACTIVIST_CACHE_PATH):
        return {}
    try:
        with open(ACTIVIST_CACHE_PATH, encoding="utf-8") as f:
            data = json.load(f)
        return data.get("by_sec_code", {})
    except Exception:
        return {}


def _has_activist(sec_code_4: str, activist_map: dict) -> bool:
    """指定銘柄がアクティビスト保有リストに含まれているか判定。"""
    return str(sec_code_4) in activist_map


def calc_governance_score_for_df(df: pd.DataFrame, code_4_col: str = "code_4") -> pd.Series:
    """
    DataFrame に対して governance_score 列を返す。

    Args:
      df: 各行に code_4 (4桁証券コード) を含む DataFrame
      code_4_col: 4桁コードのカラム名

    Returns:
      pd.Series（df.index と整合）: 各銘柄の governance_score (0〜10pt)
    """
    activist_map = _load_activist_cache()
    if not activist_map:
        # キャッシュがなければ全銘柄 0pt（governance 機能オフと同等）
        return pd.Series(0.0, index=df.index)

    scores = []
    for _, row in df.iterrows():
        code_4 = str(row.get(code_4_col, "")).strip()
        score = 0.0
        if _has_activist(code_4, activist_map):
            score += 10.0   # アクティビスト保有あり
        scores.append(score)
    return pd.Series(scores, index=df.index)


def get_activist_info(code_4: str) -> Optional[list]:
    """指定銘柄のアクティビスト保有詳細リストを返す。なければ None。
    UI 表示・claude.ai テキスト生成用。"""
    activist_map = _load_activist_cache()
    if not activist_map:
        return None
    return activist_map.get(str(code_4).strip())


def cache_status() -> dict:
    """キャッシュの状態を返す（UI 表示用）。"""
    if not os.path.exists(ACTIVIST_CACHE_PATH):
        return {"exists": False, "size_kb": 0, "n_stocks": 0, "fetched_at": None}
    try:
        with open(ACTIVIST_CACHE_PATH, encoding="utf-8") as f:
            data = json.load(f)
        return {
            "exists":     True,
            "size_kb":    os.path.getsize(ACTIVIST_CACHE_PATH) / 1024,
            "n_stocks":   len(data.get("by_sec_code", {})),
            "fetched_at": data.get("fetched_at"),
            "total_positions": data.get("total_positions"),
        }
    except Exception:
        return {"exists": False, "size_kb": 0, "n_stocks": 0, "fetched_at": None}
