# -*- coding: utf-8 -*-
"""
Claude API サービス - ポートフォリオAI分析・IR要約
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import math
import re
import time
from datetime import datetime, timedelta
from typing import Optional

import anthropic
import numpy as np
import pandas as pd

from services.jquants_service import get_ohlcv, get_financials

MODEL = "claude-haiku-4-5-20251001"

# 入力: $0.80/MTok, 出力: $4.00/MTok（Haiku 4.5）
INPUT_COST_PER_TOKEN  = 0.80 / 1_000_000
OUTPUT_COST_PER_TOKEN = 4.00 / 1_000_000


def _client() -> anthropic.Anthropic:
    key = os.getenv("ANTHROPIC_API_KEY")
    if not key:
        raise EnvironmentError(".env に ANTHROPIC_API_KEY が設定されていません")
    return anthropic.Anthropic(api_key=key)


def _nan_to_none(v):
    if isinstance(v, float) and math.isnan(v):
        return None
    if isinstance(v, (np.floating, np.integer)):
        v = v.item()
        if isinstance(v, float) and math.isnan(v):
            return None
    return v


# ---- コンテキスト構築 ----

def build_stock_context(
    stocks_df: pd.DataFrame,
    cache_df: Optional[pd.DataFrame],
    listed_df: pd.DataFrame,
) -> list[dict]:
    """
    保有銘柄ごとに Claude へ渡すデータ辞書を組み立てる。
    キャッシュにある銘柄はそこから、ない銘柄は指標なしで含める。
    """
    # listed_df: Code は5桁 → 4桁に変換してセクター/市場マップを作る
    sector_map = {}
    if not listed_df.empty and "Code" in listed_df.columns:
        tmp = listed_df.copy()
        tmp["code_4"] = tmp["Code"].str[:4]
        tmp = tmp.drop_duplicates("code_4")
        for _, row in tmp.iterrows():
            sector_map[row["code_4"]] = {
                "sector": row.get("S33Nm", row.get("S17Nm", "")),
                "market": row.get("MktNm", ""),
            }

    # cache_df: code_4 でインデックス化
    cache_map = {}
    if cache_df is not None and not cache_df.empty:
        for _, row in cache_df.iterrows():
            cache_map[str(row["code_4"])] = row

    contexts = []
    for _, srow in stocks_df.iterrows():
        code_4 = str(srow["code_4"])
        cache = cache_map.get(code_4)
        sec = sector_map.get(code_4, {})

        market_value = float(srow.get("評価額", 0) or 0)
        acq_price    = float(srow.get("取得単価", 0) or 0)
        cur_price    = float(srow.get("現在値", 0) or 0)
        pnl          = float(srow.get("損益", 0) or 0)
        pnl_pct      = float(srow.get("損益(%)", 0) or 0)
        quantity     = int(srow.get("数量", 0) or 0)

        ctx = {
            "code": code_4,
            "name": str(srow.get("会社名", "")),
            "sector": sec.get("sector", ""),
            "market": sec.get("market", ""),
            # 保有情報
            "market_value": market_value,
            "pnl": pnl,
            "pnl_pct": pnl_pct,
            "quantity": quantity,
            "acq_price": acq_price,
            "current_price": cur_price,
            # 財務・テクニカル（キャッシュから）
            "per": None,
            "pbr": None,
            "roe": None,
            "div_yield": None,
            "rev_growth": None,
            "profit_growth": None,
            "rsi": None,
            "ma25_deviation": None,
            "signal_score": None,
            "signals": "",
            "fund_score": None,
        }

        if cache is not None:
            close = _nan_to_none(cache.get("close"))
            ma25  = _nan_to_none(cache.get("MA25"))
            ctx.update({
                "per":          _nan_to_none(cache.get("PER")),
                "pbr":          _nan_to_none(cache.get("PBR")),
                "roe":          _nan_to_none(cache.get("ROE")),
                "div_yield":    _nan_to_none(cache.get("div_yield")),
                "rev_growth":   _nan_to_none(cache.get("rev_growth")),
                "profit_growth":_nan_to_none(cache.get("profit_growth")),
                "rsi":          _nan_to_none(cache.get("RSI")),
                "ma25_deviation": round((close - ma25) / ma25 * 100, 1)
                                  if close and ma25 and ma25 != 0 else None,
                "signal_score": _nan_to_none(cache.get("signal_score")),
                "signals":      str(cache.get("signals", "")) or "",
                "fund_score":   _nan_to_none(cache.get("score")),
            })

        contexts.append(ctx)

    return contexts


def fetch_fallback_metrics(code_4: str) -> dict:
    """
    キャッシュにない銘柄をJ-Quantsからリアルタイム取得して指標を計算する。
    失敗時は空dictを返す。
    """
    try:
        from screener import calc_rsi, calc_moving_average, calc_avg_volume, calc_signal_score, JQuantsClient
        import numpy as np

        code_5    = code_4 + "0"
        to_date   = datetime.today().strftime("%Y-%m-%d")
        from_date = (datetime.today() - timedelta(days=120)).strftime("%Y-%m-%d")

        ohlcv = get_ohlcv(code_5, from_date, to_date)
        if ohlcv.empty or len(ohlcv) < 20:
            return {}

        close  = ohlcv["AdjC"]
        latest = float(close.iloc[-1])
        rsi    = calc_rsi(close)
        ma25   = calc_moving_average(close, 25)
        ma25_dev = round((latest - ma25) / ma25 * 100, 1) if ma25 and ma25 != 0 else None
        sig_score, sig_labels = calc_signal_score(close)

        fin_df = get_financials(code_5)
        per = pbr = roe = div_yield = rev_growth = profit_growth = np.nan
        if not fin_df.empty:
            row = fin_df.iloc[0]
            eps    = pd.to_numeric(row.get("EPS"),     errors="coerce")
            eq     = pd.to_numeric(row.get("Eq"),      errors="coerce")
            sh_out = pd.to_numeric(row.get("ShOutFY"), errors="coerce")
            np_val = pd.to_numeric(row.get("NP"),      errors="coerce")
            div    = pd.to_numeric(row.get("FDivAnn"), errors="coerce")
            if np.isnan(div):
                div = pd.to_numeric(row.get("DivAnn"), errors="coerce")

            per = latest / eps if not np.isnan(eps) and eps > 0 else np.nan
            bps = eq / sh_out  if not np.isnan(eq) and not np.isnan(sh_out) and sh_out > 0 else np.nan
            pbr = latest / bps if not np.isnan(bps) and bps > 0 else np.nan
            roe = np_val / eq * 100 if not np.isnan(np_val) and not np.isnan(eq) and eq > 0 else np.nan
            div_yield = div / latest * 100 if not np.isnan(div) and latest > 0 else np.nan

            if len(fin_df) >= 2:
                prev  = fin_df.iloc[1]
                rev_c = pd.to_numeric(row.get("Sales"),  errors="coerce")
                rev_p = pd.to_numeric(prev.get("Sales"), errors="coerce")
                prf_c = pd.to_numeric(row.get("NP"),     errors="coerce")
                prf_p = pd.to_numeric(prev.get("NP"),    errors="coerce")
                if not np.isnan(rev_p) and rev_p != 0:
                    rev_growth = (rev_c - rev_p) / abs(rev_p) * 100
                if not np.isnan(prf_p) and prf_p != 0:
                    profit_growth = (prf_c - prf_p) / abs(prf_p) * 100

        return {
            "per":           _nan_to_none(per),
            "pbr":           _nan_to_none(pbr),
            "roe":           _nan_to_none(roe),
            "div_yield":     _nan_to_none(div_yield),
            "rev_growth":    _nan_to_none(rev_growth),
            "profit_growth": _nan_to_none(profit_growth),
            "rsi":           _nan_to_none(rsi),
            "ma25_deviation":ma25_dev,
            "signal_score":  sig_score,
            "signals":       ", ".join(sig_labels) if sig_labels else "",
        }
    except Exception:
        return {}


# ---- プロンプト構築 ----

_SYSTEM_PROMPT = """あなたは日本株投資の専門アナリストです。
提供されたポートフォリオデータを分析し、以下の形式で必ずJSONのみを出力してください。

{
  "overall": {
    "summary": "ポートフォリオ全体の総評（200文字以内）",
    "risk_level": "低|中|高",
    "sector_bias": "セクター偏りの説明（100文字以内）",
    "strengths": ["強み1", "強み2"],
    "weaknesses": ["弱み1", "弱み2"]
  },
  "stocks": [
    {
      "code": "4桁の銘柄コード",
      "action": "買い増し推奨|継続保有|利確検討|損切り検討",
      "reason": "判断理由（100文字以内）",
      "focus": "注目ポイント（50文字以内）"
    }
  ],
  "actions": ["今すぐやること1", "今すぐやること2", "今すぐやること3"]
}

分析の観点:
- ファンダメンタルズ: PER・PBR・ROE・配当利回り・売上/利益成長率
- テクニカル: RSI・MA25乖離率・シグナルスコア・買い/売りシグナル
- ポートフォリオ全体: セクター集中リスク・含み益/損のバランス・損切り/利確候補
- 売買提案: ファンダとテクニカルの両面から根拠ある提案をしてください

JSON以外のテキストは一切出力しないでください。"""


def build_prompt(stock_contexts: list[dict], portfolio_summary: dict) -> str:
    def fmt(v, decimals=1, suffix=""):
        if v is None:
            return "データなし"
        return f"{v:.{decimals}f}{suffix}"

    total_value   = portfolio_summary.get("total_value", 0)
    total_pnl_pct = portfolio_summary.get("total_pnl_pct", 0)
    count         = portfolio_summary.get("count", 0)
    sectors       = portfolio_summary.get("sectors", {})
    sector_str    = "、".join([f"{k}({v}銘柄)" for k, v in sectors.items()]) or "不明"

    lines = [
        "## ポートフォリオ全体",
        f"- 総評価額: {total_value:,.0f}円",
        f"- 総損益率: {total_pnl_pct:+.2f}%",
        f"- 保有銘柄数: {count}銘柄",
        f"- セクター: {sector_str}",
        "",
        "## 保有銘柄データ",
    ]

    for ctx in stock_contexts:
        lines.append(f"\n### {ctx['code']} {ctx['name']}（{ctx['sector']} / {ctx['market']}）")
        lines.append(f"- 評価額: {ctx['market_value']:,.0f}円 / 損益: {ctx['pnl_pct']:+.1f}%")
        lines.append(f"- PER: {fmt(ctx['per'],'1','倍')} / PBR: {fmt(ctx['pbr'],'2','倍')} / ROE: {fmt(ctx['roe'],'1','%')}")
        lines.append(f"- 配当利回り: {fmt(ctx['div_yield'],'1','%')} / 売上成長: {fmt(ctx['rev_growth'],'1','%')} / 利益成長: {fmt(ctx['profit_growth'],'1','%')}")
        lines.append(f"- RSI: {fmt(ctx['rsi'],'1')} / MA25乖離: {fmt(ctx['ma25_deviation'],'1','%')} / シグナルスコア: {fmt(ctx['signal_score'],'0','点')}")
        lines.append(f"- ファンダスコア: {fmt(ctx['fund_score'],'1','点')} / シグナル: {ctx['signals'] or 'なし'}")

    lines.append("\n上記データを分析し、JSONで出力してください。")
    return "\n".join(lines)


# ---- Claude API 呼び出し ----

def analyze_portfolio(
    stock_contexts: list[dict],
    portfolio_summary: dict,
) -> dict:
    """
    Claude API を呼び出してポートフォリオ分析結果を返す。

    Returns:
        {
            "overall": {...},
            "stocks": [...],
            "actions": [...],
            "input_tokens": int,
            "output_tokens": int,
        }
    """
    client = _client()
    prompt = build_prompt(stock_contexts, portfolio_summary)

    response = client.messages.create(
        model=MODEL,
        max_tokens=2048,
        system=_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}],
    )

    text = response.content[0].text.strip()

    # JSON抽出（前後の余分なテキストがある場合も対応）
    match = re.search(r'\{.*\}', text, re.DOTALL)
    if not match:
        raise ValueError(f"JSONが見つかりません。レスポンス: {text[:200]}")

    result = json.loads(match.group())
    result["input_tokens"]  = response.usage.input_tokens
    result["output_tokens"] = response.usage.output_tokens
    return result


# ---- ユーティリティ ----

ACTION_MAP = {
    "買い増し推奨": ("📈", "#26a69a"),
    "継続保有":     ("✅", "#2196f3"),
    "利確検討":     ("💰", "#ff9800"),
    "損切り検討":   ("🔻", "#ef5350"),
}


def parse_action_label(action: str) -> tuple[str, str]:
    """アクションラベルを (絵文字, カラーコード) に変換する"""
    return ACTION_MAP.get(action, ("❓", "#7a8499"))


def calc_cost(input_tokens: int, output_tokens: int) -> float:
    """推定コスト（USD）を返す"""
    return input_tokens * INPUT_COST_PER_TOKEN + output_tokens * OUTPUT_COST_PER_TOKEN


# ---- IR要約 ----

_IR_SYSTEM_PROMPT = """あなたは日本株投資の専門アナリストです。
提供された適時開示文書（決算短信・業績修正等）を読んで、個人投資家向けに要約してください。
必ず以下のJSON形式のみで出力してください。

{
  "headline": "一言で言うと何の発表か（30文字以内）",
  "summary": "内容の要約（200文字以内）",
  "impact": "株価への影響見通し（ポジティブ/ニュートラル/ネガティブ）",
  "impact_reason": "影響の理由（100文字以内）",
  "key_numbers": ["注目数値1（例: 売上高+15%）", "注目数値2", "注目数値3"],
  "investor_action": "個人投資家が取るべきアクション（100文字以内）",
  "tone_bullish": 経営陣の強気度（1〜10の整数。10が最も強気）,
  "tone_certainty": 業績の確実性（1〜10の整数。受注残・契約済み等の根拠が強いほど高い）,
  "tone_surprise": サプライズ度（1〜10の整数。市場予想や前回予想を大きく上回るほど高い）
}

トーンスコアの採点基準:
- tone_bullish: 「来期も増収増益」「積極投資」→高い / 「慎重に見ている」「不透明」→低い
- tone_certainty: 「受注残が積み上がり」「長期契約獲得」→高い / 「見通し不透明」→低い
- tone_surprise: 「市場予想を大幅上回る」「前回予想から大幅上方修正」→高い / 計画通り→低い

JSON以外のテキストは一切出力しないでください。"""


def summarize_ir(title: str, company_name: str, pdf_text: str) -> dict:
    """
    適時開示文書をClaude APIで要約する。

    Args:
        title: 開示タイトル
        company_name: 会社名
        pdf_text: PDFから抽出したテキスト（空の場合はタイトルのみで要約）

    Returns:
        {headline, summary, impact, impact_reason, key_numbers, investor_action,
         input_tokens, output_tokens}
    """
    client = _client()

    if pdf_text:
        user_content = (
            f"会社名: {company_name}\n"
            f"開示タイトル: {title}\n\n"
            f"--- 文書本文 ---\n{pdf_text}"
        )
    else:
        user_content = (
            f"会社名: {company_name}\n"
            f"開示タイトル: {title}\n\n"
            "※文書本文は取得できませんでした。タイトルから推定して要約してください。"
        )

    response = client.messages.create(
        model=MODEL,
        max_tokens=1024,
        system=_IR_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_content}],
    )

    text = response.content[0].text.strip()
    match = re.search(r'\{.*\}', text, re.DOTALL)
    if not match:
        raise ValueError(f"JSONが見つかりません。レスポンス: {text[:200]}")

    result = json.loads(match.group())
    result["input_tokens"] = response.usage.input_tokens
    result["output_tokens"] = response.usage.output_tokens
    return result


IMPACT_COLOR = {
    "ポジティブ": "#26a69a",
    "ニュートラル": "#7a8499",
    "ネガティブ": "#ef5350",
}
