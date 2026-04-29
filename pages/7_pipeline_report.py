# -*- coding: utf-8 -*-
"""
パイプラインレポート
  ① ハードフィルタ → ② ファンダスコア → ③ テクニカルスコア → Claude AI分析
  結果を画面表示 + HTML書き出し（Dropbox保存）
"""

import os
import json
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

from services.pipeline_service import run_pipeline

# ── ページ設定 ─────────────────────────────────────────────────────────
st.set_page_config(
    page_title="パイプラインレポート",
    page_icon="🔬",
    layout="wide",
)

# ── パス設定 ──────────────────────────────────────────────────────────
_ROOT       = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_CACHE_PATH = os.path.join(_ROOT, "data", "pipeline_cache.json")
_DROPBOX    = os.path.expanduser("~/Dropbox")
_REPORT_DIR = os.path.join(_DROPBOX, "投資レポート")


# ── キャッシュ永続化 ───────────────────────────────────────────────────

_NUMERIC_COLS = [
    "close", "funda_score", "tech_score", "total_score",
    "rev_growth", "profit_growth", "ROE", "PER", "PBR", "market_cap",
    "stop_loss", "stop_pct", "target", "target2", "entry_breakout", "entry_pullback",
    "sepa_stage", "mom_revision", "div_trend", "op_trend", "payout_ratio",
    "op_turnaround",
]


def _save_cache(result: dict) -> None:
    """top10・scored・ai_analysis・market_condition・stats・mode をJSONに保存する。"""
    scored = result.get("scored", pd.DataFrame())
    payload = {
        "generated_at":     datetime.now().isoformat(),
        "mode":             result.get("mode", "growth"),
        "stats":            result["stats"],
        "top10":            result["top10"].to_dict(orient="records"),
        "scored":           scored.to_dict(orient="records") if not scored.empty else [],
        "ai_analysis":      result["ai_analysis"],
        "market_condition": result.get("market_condition"),
    }
    with open(_CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, default=str)


def _restore_df(records: list) -> pd.DataFrame:
    """JSON レコードリストを DataFrame に復元し数値列を変換する。"""
    df = pd.DataFrame(records)
    for col in _NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _load_cache() -> dict:
    """保存済みキャッシュを読み込む。なければ None を返す。"""
    if not os.path.exists(_CACHE_PATH):
        return None
    try:
        with open(_CACHE_PATH, "r", encoding="utf-8") as f:
            payload = json.load(f)
        return {
            "generated_at":     payload.get("generated_at", ""),
            "mode":             payload.get("mode", "growth"),
            "stats":            payload["stats"],
            "top10":            _restore_df(payload["top10"]),
            "scored":           _restore_df(payload.get("scored", [])),
            "ai_analysis":      payload.get("ai_analysis"),
            "market_condition": payload.get("market_condition"),
        }
    except Exception:
        return None

# ════════════════════════════════════════════════════════════════════════
#  ヘルパー
# ════════════════════════════════════════════════════════════════════════

def _fmt(v, decimals=1, suffix="", default="N/A"):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return default
    return f"{v:.{decimals}f}{suffix}"


def _judgment_color(j: str) -> str:
    if "買い" in j:
        return "#26a69a"
    if "監視" in j:
        return "#ff9800"
    return "#ef5350"


_SIG_COLOR = {"BUY": "#26a69a", "WATCH": "#ff9800", "AVOID": "#ef5350"}
_SIG_LABEL = {"BUY": "BUY", "WATCH": "WATCH", "AVOID": "AVOID"}


def _render_scorecard(rank: int, row, ai_stocks: dict, key_prefix: str = "t1", mode: str = "growth") -> None:
    """1銘柄分のスコアカードを描画する。key_prefix でタブ間のID重複を防ぐ。"""
    ai_data   = ai_stocks.get(getattr(row, "code_4", ""), {})
    judgment  = ai_data.get("judgment", "")
    jcolor    = _judgment_color(judgment)
    signal       = getattr(row, "signal", "")
    sig_color    = _SIG_COLOR.get(signal, "#888")
    sig_label    = _SIG_LABEL.get(signal, signal)
    sepa_stage   = getattr(row, "sepa_stage", 0)
    is_sepa2     = sepa_stage == 2
    mom_revision = getattr(row, "mom_revision", None)
    has_revision = mom_revision is not None and not (isinstance(mom_revision, float) and np.isnan(mom_revision))

    with st.container(border=True):
        hc1, hc2, hc3 = st.columns([0.5, 3.5, 1.2])
        hc1.markdown(
            f'<p style="font-size:1.5rem;font-weight:700;margin:0.2rem 0 0 0">#{rank}</p>',
            unsafe_allow_html=True,
        )
        hc2.markdown(
            f'<p style="font-size:1.5rem;font-weight:700;margin:0.2rem 0 0 0">'
            f'{row.code_4}&nbsp;&nbsp;{row.company_name}'
            f'&nbsp;&nbsp;<span style="color:#aaa;font-size:0.85rem;font-weight:400">'
            f'{row.sector} / {row.market}</span></p>',
            unsafe_allow_html=True,
        )
        badge_html = (
            f'<p style="margin:0.4rem 0 0 0">'
            f'<span style="background:{sig_color};color:#fff;padding:4px 10px;'
            f'border-radius:12px;font-size:0.75rem;margin-right:6px">{sig_label}</span>'
        )
        if judgment:
            badge_html += (
                f'<span style="background:{jcolor};color:#fff;padding:4px 10px;'
                f'border-radius:12px;font-size:0.75rem;margin-right:6px">{judgment}</span>'
            )
        if has_revision:
            badge_html += (
                f'<span style="background:#7c3aed;color:#fff;padding:4px 10px;'
                f'border-radius:12px;font-size:0.75rem">上方修正+{mom_revision:.0f}%</span>'
            )
        badge_html += '</p>'
        hc3.markdown(badge_html, unsafe_allow_html=True)

        m1, m2, m3, m4, m5, m6 = st.columns(6)
        m1.metric("総合スコア", f"{row.total_score:.1f}")
        m2.metric("ファンダ",   f"{row.funda_score:.1f}")
        m3.metric("テクニカル", f"{row.tech_score:.1f}")
        m4.metric("株価",       f"¥{row.close:,.0f}")
        m5.metric("ROE",        f"{row.ROE:.1f}%")
        m6.metric("PER",        f"{row.PER:.1f}x")

        d1, d2 = st.columns(2)
        with d1:
            st.caption(
                f"売上成長 **{row.rev_growth:.1f}%** ／ 利益成長 **{row.profit_growth:.1f}%** ／ "
                f"PBR **{row.PBR:.1f}x**"
            )
        with d2:
            detail   = row.tech_detail if isinstance(row.tech_detail, dict) else {}
            rsi_v    = detail.get("rsi", "N/A")
            ma25_v   = detail.get("ma25", "N/A")
            altman_z = getattr(row, "altman_z", None)
            gran_g1  = getattr(row, "gran_g1",  False)
            gran_g2  = getattr(row, "gran_g2",  False)
            dow_up   = getattr(row, "dow_uptrend", False)

            ma200_v    = detail.get("ma200",    None)
            ma200_dev  = detail.get("ma200_dev", None)
            div_trend     = int(getattr(row, "div_trend",    0) or 0)
            op_trend      = int(getattr(row, "op_trend",     0) or 0)
            op_turnaround = bool(getattr(row, "op_turnaround", False) or False)
            payout_r      = getattr(row, "payout_ratio", None)
            vol_trend_r   = detail.get("vol_trend_ratio", None)

            ind_parts = []
            # SEPA ステージ
            sepa_color = "#26a69a" if is_sepa2 else "#666"
            ind_parts.append(
                f'<span style="color:{sepa_color}">SEPA<b>{sepa_stage}</b></span>'
            )
            if altman_z is not None and pd.notna(altman_z):
                az_color = "#26a69a" if altman_z >= 3.0 else ("#ff9800" if altman_z >= 1.8 else "#ef5350")
                ind_parts.append(
                    f'<span style="color:{az_color}">AltmanZ:<b>{altman_z:.2f}</b></span>'
                )
            gran_labels = []
            if gran_g1: gran_labels.append("G1")
            if gran_g2: gran_labels.append("G2")
            if gran_labels:
                ind_parts.append(f'グランビル:<b>{"/".join(gran_labels)}</b>')
            if dow_up:
                ind_parts.append('<span style="color:#26a69a">ダウ:<b>上昇</b></span>')

            ind_str = ("　｜　" + "　｜　".join(ind_parts)) if ind_parts else ""

            # バリューモード追加指標
            val_extra = ""
            if mode == "value":
                # MA200乖離率（-5%以上の場合のみ表示）
                if ma200_v is not None and ma200_dev is not None and ma200_dev >= -5:
                    dev_str = f"({ma200_dev:+.1f}%)"
                    if 0 <= ma200_dev <= 5:
                        dev_col = "#26a69a"   # 緑: 最高ゾーン
                    elif -5 <= ma200_dev < 0:
                        dev_col = "#0891b2"   # 青: 上抜け直前
                    else:
                        dev_col = "#ff9800"   # 橙: 出遅れ気味
                    val_extra += f'　／　MA200 <b style="color:{dev_col}">{ma200_v}{dev_str}</b>'
                # V字転換（op_turnaround 優先表示）
                if op_turnaround:
                    val_extra += f'　／　<span style="color:#f59e0b"><b>V字転換</b></span>'
                elif op_trend >= 1:
                    op_label = "営業益2期連続増" if op_trend >= 2 else "営業益増"
                    val_extra += f'　／　<span style="color:#26a69a"><b>{op_label}</b></span>'
                # 増配トレンド
                if div_trend >= 1:
                    div_label = "2期連続増配" if div_trend >= 2 else "増配"
                    val_extra += f'　／　<span style="color:#26a69a"><b>{div_label}</b></span>'
                # 配当性向（上限70%のみ）
                if payout_r is not None and not (isinstance(payout_r, float) and np.isnan(payout_r)):
                    pr_col = "#26a69a" if 0 < payout_r <= 70 else "#ff9800"
                    val_extra += f'　／　配当性向 <b style="color:{pr_col}">{payout_r:.0f}%</b>'
                # 出来高トレンド比率
                if vol_trend_r is not None:
                    vt_col = "#26a69a" if vol_trend_r >= 1.3 else "#aaa"
                    val_extra += f'　／　需給 <b style="color:{vt_col}">{vol_trend_r:.2f}x</b>'

            st.markdown(
                f'<span style="font-size:0.78rem;color:#aaa">RSI <b>{rsi_v}</b>　／　MA25 <b>{ma25_v}</b>{val_extra}{ind_str}</span>',
                unsafe_allow_html=True,
            )

        if ai_data:
            story_label = "割安シナリオ" if mode == "value" else "成長ストーリー"
            if ai_data.get("story"):
                st.markdown(f"**{story_label}**: {ai_data['story']}")
            ai_c1, ai_c2, ai_c3 = st.columns(3)
            if ai_data.get("upside"):
                ai_c1.success(f"上昇余地: {ai_data['upside']}")
            if ai_data.get("catalyst"):
                ai_c2.info(f"カタリスト: {ai_data['catalyst']}")
            if ai_data.get("risk"):
                ai_c3.warning(f"リスク: {ai_data['risk']}")

        if signal in ("BUY", "WATCH"):
            stop    = getattr(row, "stop_loss", None)
            stopp   = getattr(row, "stop_pct",  None)
            tgt     = getattr(row, "target",    None)
            tgt2    = getattr(row, "target2",   None)
            tgt_pct = getattr(row, "target_pct", 25)
            eb      = getattr(row, "entry_breakout", None)
            ep      = getattr(row, "entry_pullback", None)
            sreason = getattr(row, "signal_reason", "")
            has_tgt2 = (tgt2 is not None) and not (isinstance(tgt2, float) and np.isnan(tgt2))
            if has_tgt2:
                pc1, pc2, pc3, pc4, pc5 = st.columns(5)
            else:
                pc1, pc2, pc3, pc4 = st.columns(4)
            if eb and pd.notna(eb):   pc1.metric("ブレイクエントリー", f"¥{eb:,.0f}")
            if ep and pd.notna(ep):   pc2.metric("押し目エントリー",   f"¥{ep:,.0f}")
            if stop: pc3.metric("損切りライン", f"¥{stop:,.0f}",
                                delta=f"{stopp:.1f}%" if stopp else None,
                                delta_color="inverse")
            tgt_label = f"第1利確 (+{tgt_pct:.0f}%)" if has_tgt2 else f"利確目標 (+{tgt_pct:.0f}%)"
            if tgt and pd.notna(tgt):  pc4.metric(tgt_label, f"¥{tgt:,.0f}")
            if has_tgt2:               pc5.metric("第2利確 (+40%)", f"¥{tgt2:,.0f}")
            if sreason and signal == "WATCH":
                st.caption(f"WATCH理由: {sreason}")

        detail = row.tech_detail if isinstance(row.tech_detail, dict) else {}
        if detail:
            with st.expander("テクニカル内訳"):
                st.plotly_chart(
                    _chart_radar(pd.Series({"company_name": row.company_name}), detail),
                    use_container_width=True,
                    key=f"radar_{key_prefix}_{rank}_{row.code_4}",
                )


# ════════════════════════════════════════════════════════════════════════
#  グラフ描画
# ════════════════════════════════════════════════════════════════════════

def _chart_score_bar(top10: pd.DataFrame):
    """総合スコア棒グラフ。"""
    fig = px.bar(
        top10,
        x="total_score",
        y="company_name",
        orientation="h",
        color="total_score",
        color_continuous_scale="teal",
        labels={"total_score": "総合スコア", "company_name": ""},
        title="総合スコア TOP10",
    )
    fig.update_layout(height=380, yaxis={"autorange": "reversed"},
                      coloraxis_showscale=False, margin=dict(l=0, r=20, t=40, b=0))
    return fig


def _chart_scatter(top10: pd.DataFrame):
    """ファンダ vs テクニカル 散布図。"""
    fig = px.scatter(
        top10,
        x="funda_score",
        y="tech_score",
        size="total_score",
        color="total_score",
        color_continuous_scale="teal",
        hover_name="company_name",
        text="code_4",
        labels={"funda_score": "ファンダスコア", "tech_score": "テクニカルスコア"},
        title="ファンダ vs テクニカル",
    )
    fig.update_traces(textposition="top center")
    fig.update_layout(height=380, coloraxis_showscale=False,
                      margin=dict(l=0, r=20, t=40, b=0))
    return fig


def _chart_radar(row: pd.Series, detail: dict):
    """1銘柄のテクニカル内訳レーダー。"""
    cats    = ["MA", "RSI", "MACD", "出来高", "高値ブレイク"]
    maxvals = [30, 20, 20, 15, 15]
    vals    = [
        detail.get("ma_score",    0),
        detail.get("rsi_score",   0),
        detail.get("macd_score",  0),
        detail.get("vol_score",   0),
        detail.get("break_score", 0),
    ]
    pct = [v / m * 100 for v, m in zip(vals, maxvals)]

    fig = go.Figure(go.Scatterpolar(
        r=pct + [pct[0]],
        theta=cats + [cats[0]],
        fill="toself",
        fillcolor="rgba(8,145,178,0.3)",
        line_color="#0891b2",
    ))
    fig.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
        showlegend=False,
        height=280,
        margin=dict(l=30, r=30, t=30, b=30),
        title=dict(text=f"{row['company_name']} テクニカル内訳", font_size=13),
    )
    return fig


# ════════════════════════════════════════════════════════════════════════
#  HTMLレポート生成
# ════════════════════════════════════════════════════════════════════════

def _generate_html(top10: pd.DataFrame, ai: dict, stats: dict, mode: str = "growth") -> str:
    date_str   = datetime.now().strftime("%Y-%m-%d %H:%M")
    mode_label = "バリュー株" if mode == "value" else "成長株"

    # スコアカード HTML
    cards_html = ""
    ai_stocks  = {s["code"]: s for s in ai.get("stocks", [])} if ai else {}

    for rank, row in enumerate(top10.itertuples(), 1):
        ai_data   = ai_stocks.get(row.code_4, {})
        judgment  = ai_data.get("judgment", "")
        jcolor    = _judgment_color(judgment)
        story     = ai_data.get("story", "")
        upside    = ai_data.get("upside", "")
        risk      = ai_data.get("risk", "")
        catalyst  = ai_data.get("catalyst", "")
        is_sepa2  = getattr(row, "sepa_stage", 0) == 2
        sepa2_html = '<span class="sepa2-badge">SEPA2</span>' if is_sepa2 else ""
        judg_html  = f'<span class="judgment" style="background:{jcolor}">{judgment}</span>' if judgment else ""

        cards_html += f"""
        <div class="card">
          <div class="card-header">
            <span class="rank">#{rank}</span>
            <span class="code">{row.code_4}</span>
            <span class="name">{row.company_name}</span>
            {sepa2_html}{judg_html}
          </div>
          <div class="scores">
            <div class="score-item"><span class="label">総合</span><span class="value total">{row.total_score:.1f}</span></div>
            <div class="score-item"><span class="label">ファンダ</span><span class="value">{row.funda_score:.1f}</span></div>
            <div class="score-item"><span class="label">テクニカル</span><span class="value">{row.tech_score:.1f}</span></div>
            <div class="score-item"><span class="label">株価</span><span class="value">¥{row.close:,.0f}</span></div>
            <div class="score-item"><span class="label">ROE</span><span class="value">{row.ROE:.1f}%</span></div>
            <div class="score-item"><span class="label">PER</span><span class="value">{row.PER:.1f}x</span></div>
          </div>
          <div class="metrics">
            売上成長 {row.rev_growth:.1f}% ／ 利益成長 {row.profit_growth:.1f}%
          </div>
          {'<div class="story">' + story + '</div>' if story else ''}
          {'<div class="upside">上昇余地: ' + upside + '</div>' if upside else ''}
          {'<div class="risk">リスク: ' + risk + '</div>' if risk else ''}
          {'<div class="catalyst">カタリスト: ' + catalyst + '</div>' if catalyst else ''}
        </div>
        """

    top3        = ai.get("top3", []) if ai else []
    top3_reason = ai.get("top3_reason", "") if ai else ""
    market_comment = ai.get("market_comment", "") if ai else ""

    # f-string内でバックスラッシュ不可（Python 3.9）のため事前生成
    market_html = ('<div class="market-comment">' + market_comment + "</div>") if market_comment else ""

    top3_badges = "".join('<span class="badge">' + c + "</span>" for c in top3)
    top3_html   = (
        '<div class="section-title">最有望ベスト3</div>'
        '<div class="top3"><div class="badges">' + top3_badges +
        '</div><div class="reason">' + top3_reason + "</div></div>"
    ) if top3 else ""

    rows_html = ""
    for i, r in enumerate(top10.itertuples()):
        rows_html += (
            "<tr>"
            "<td>" + str(i + 1) + "</td>"
            "<td>" + str(r.company_name) + "</td>"
            "<td>¥" + f"{r.close:,.0f}" + "</td>"
            "<td>" + f"{r.rev_growth:.1f}" + "</td>"
            "<td>" + f"{r.profit_growth:.1f}" + "</td>"
            "<td>" + f"{r.ROE:.1f}" + "</td>"
            "<td>" + f"{r.PER:.1f}" + "</td>"
            "<td>" + f"{r.funda_score:.1f}" + "</td>"
            "<td>" + f"{r.tech_score:.1f}" + "</td>"
            "<td><b>" + f"{r.total_score:.1f}" + "</b></td>"
            "</tr>"
        )

    return f"""<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{mode_label}パイプラインレポート {date_str}</title>
  <style>
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
           background: #0f1117; color: #e0e0e0; padding: 16px; }}
    h1 {{ font-size: 1.4rem; color: #0891b2; margin-bottom: 4px; }}
    .meta {{ font-size: 0.8rem; color: #888; margin-bottom: 16px; }}
    .stats {{ display: flex; gap: 12px; margin-bottom: 20px; }}
    .stat {{ background: #1e2130; border-radius: 8px; padding: 12px 16px; flex: 1; text-align: center; }}
    .stat .n {{ font-size: 1.6rem; font-weight: bold; color: #0891b2; }}
    .stat .l {{ font-size: 0.75rem; color: #888; }}
    .market-comment {{ background: #1e2130; border-left: 3px solid #0891b2;
                       padding: 12px; border-radius: 4px; margin-bottom: 20px;
                       font-size: 0.9rem; line-height: 1.6; }}
    .section-title {{ font-size: 1.1rem; font-weight: bold; color: #0891b2;
                      margin: 20px 0 10px; border-bottom: 1px solid #333; padding-bottom: 6px; }}
    .cards {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(320px, 1fr)); gap: 12px; }}
    .card {{ background: #1e2130; border-radius: 10px; padding: 14px; }}
    .card-header {{ display: flex; align-items: center; gap: 8px; margin-bottom: 10px; flex-wrap: wrap; }}
    .rank {{ font-size: 1.1rem; font-weight: bold; color: #0891b2; min-width: 28px; }}
    .code {{ font-size: 0.85rem; background: #2d3250; padding: 2px 8px; border-radius: 4px; }}
    .name {{ font-size: 0.95rem; font-weight: bold; flex: 1; }}
    .judgment {{ font-size: 0.75rem; padding: 3px 8px; border-radius: 12px; color: #fff; }}
    .sepa2-badge {{ font-size: 0.75rem; padding: 3px 8px; border-radius: 12px;
                   color: #fff; background: #7c3aed; }}
    .scores {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 6px; margin-bottom: 8px; }}
    .score-item {{ background: #252840; border-radius: 6px; padding: 6px; text-align: center; }}
    .score-item .label {{ font-size: 0.65rem; color: #888; display: block; }}
    .score-item .value {{ font-size: 0.95rem; font-weight: bold; }}
    .score-item .total {{ color: #0891b2; font-size: 1.1rem; }}
    .metrics {{ font-size: 0.78rem; color: #aaa; margin-bottom: 8px; }}
    .story {{ font-size: 0.82rem; line-height: 1.6; margin-bottom: 6px; color: #ccc; }}
    .upside {{ font-size: 0.8rem; color: #26a69a; margin-bottom: 4px; }}
    .risk {{ font-size: 0.8rem; color: #ef9a9a; margin-bottom: 4px; }}
    .catalyst {{ font-size: 0.8rem; color: #ffcc80; }}
    .top3 {{ background: #1e2130; border-radius: 10px; padding: 16px; margin-bottom: 20px; }}
    .top3 .badges {{ display: flex; gap: 10px; margin-bottom: 10px; flex-wrap: wrap; }}
    .top3 .badge {{ background: #0891b2; color: #fff; padding: 4px 14px;
                   border-radius: 20px; font-weight: bold; font-size: 0.9rem; }}
    .top3 .reason {{ font-size: 0.85rem; line-height: 1.7; color: #ccc; }}
    .table-wrap {{ overflow-x: auto; margin-bottom: 20px; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 0.8rem; }}
    th {{ background: #252840; padding: 8px; text-align: right; color: #888; white-space: nowrap; }}
    th:first-child, th:nth-child(2) {{ text-align: left; }}
    td {{ padding: 7px 8px; border-bottom: 1px solid #252840; text-align: right; white-space: nowrap; }}
    td:first-child, td:nth-child(2) {{ text-align: left; }}
    tr:hover td {{ background: #252840; }}
  </style>
</head>
<body>
  <h1>{mode_label}パイプラインレポート</h1>
  <div class="meta">生成日時: {date_str}　｜
    全銘柄: {stats['total']:,} → フィルタ通過: {stats['filtered']} → 最終: {stats['top10']}</div>

  <div class="stats">
    <div class="stat"><div class="n">{stats['total']:,}</div><div class="l">全銘柄</div></div>
    <div class="stat"><div class="n">{stats['filtered']}</div><div class="l">フィルタ通過</div></div>
    <div class="stat"><div class="n">{stats['top10']}</div><div class="l">最終候補</div></div>
  </div>

  {market_html}

  {top3_html}

  <div class="section-title">スコアランキング TOP{stats['top10']}</div>
  <div class="table-wrap">
    <table>
      <tr>
        <th>#</th><th>会社名</th><th>株価</th><th>売上成長%</th><th>利益成長%</th>
        <th>ROE%</th><th>PER</th><th>ファンダ</th><th>テクニカル</th><th>総合</th>
      </tr>
      {rows_html}
    </table>
  </div>

  <div class="section-title">銘柄詳細分析</div>
  <div class="cards">{cards_html}</div>

</body>
</html>"""


def _save_html(html: str) -> str:
    """Dropbox の投資レポートフォルダに保存してパスを返す。"""
    os.makedirs(_REPORT_DIR, exist_ok=True)
    fname = f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M')}.html"
    path  = os.path.join(_REPORT_DIR, fname)
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
    return path


# ════════════════════════════════════════════════════════════════════════
#  Streamlit UI
# ════════════════════════════════════════════════════════════════════════

st.title("🔬 パイプラインレポート")
st.caption("ハードフィルタ → ファンダスコア → テクニカルスコア → Claude AI分析")

# ── サイドバー ─────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 設定")
    mode = st.radio(
        "モード",
        options=["growth", "value"],
        format_func=lambda x: "📈 成長株モード" if x == "growth" else "💎 バリュー株モード",
        horizontal=True,
        key="pipeline_mode",
    )
    use_claude = st.toggle("Claude AI分析を実行する", value=True)
    if use_claude:
        st.caption("上位10銘柄を Sonnet 4.6 で分析します（1回あたり数十円）")
    run_btn = st.button("🚀 パイプライン実行", use_container_width=True, type="primary")

    st.markdown("---")
    st.markdown("**設計仕様**")
    if mode == "value":
        st.markdown("""
- **ハードフィルタ（バリュー）**
  - PBR ≤ 1.5 / PER ≤ 25（かつ正値）
  - ROE ≥ 8% / 自己資本比率 ≥ 40%
  - 時価総額 > 100億 / 売上成長 ≥ 3%
  - シクリカル5業種除外（鉄鋼/海運業/その他製品/鉱業/ゴム製品）
- **ファンダ** (60%)
  - Value 50% / Quality 25% / Growth 25%
  - V字転換+15pt / 2期増益+10pt / 増配+10pt
- **テクニカル** (40%)
  - MA200乖離 30pt / RSI 20pt（30-50）
  - MACD 20pt / 需給 15pt / 高値ブレイク 15pt
- **BUYシグナル条件**
  - 総合≥60 / テクニカル≥55 / RSI 30-50
- **利確目標**
  - 第1目標 +35% / 第2目標 +40%
  - 損切り -15%（or MA25の高い方）
- **⚠️ ローテーションルール**
  - 市場が強気転換時は成長株モードへ切替推奨
""")
    else:
        st.markdown("""
- **ハードフィルタ（成長株）**
  - 売上成長率 > 10%
  - 利益成長率 > 10%
  - ROE > 15%
  - 自己資本比率 > 30%
  - 時価総額 > 100億
- **ファンダ** (60%)
  - Growth 50% / Quality 25% / Value 25%
- **テクニカル** (40%)
  - MA 30pt / RSI 20pt（50-65）/ MACD 20pt
  - 出来高 15pt / 高値ブレイク 15pt
""")

# ── セッション管理（リロード時はキャッシュから復元） ──────────────────
if "pipeline_result" not in st.session_state:
    st.session_state.pipeline_result = _load_cache()

if run_btn:
    with st.status("パイプライン実行中...", expanded=True) as status:
        log = st.empty()
        def _cb(msg):
            log.write(f"⏳ {msg}")

        try:
            result = run_pipeline(use_claude=use_claude, progress_callback=_cb, mode=mode)
            result["generated_at"] = datetime.now().isoformat()
            # Claude分析OFFの場合、既存のAI分析結果を引き継ぐ
            if not use_claude:
                prev = st.session_state.get("pipeline_result") or {}
                prev_ai = prev.get("ai_analysis")
                if prev_ai and not prev_ai.get("error"):
                    result["ai_analysis"] = prev_ai
            _save_cache(result)
            st.session_state.pipeline_result = result
            status.update(label="✅ 完了", state="complete")
        except Exception as e:
            status.update(label=f"❌ エラー: {e}", state="error")
            st.exception(e)

result = st.session_state.pipeline_result

if result is None:
    st.info("サイドバーの「🚀 パイプライン実行」ボタンを押してください。")
    st.stop()

top10       = result["top10"]
scored      = result.get("scored", pd.DataFrame())
ai          = result["ai_analysis"]
stats       = result["stats"]
market      = result.get("market_condition") or {}
cached_mode = result.get("mode", "growth")
generated_at = result.get("generated_at", "")

# キャッシュのモードとサイドバー選択が異なる場合に通知
if cached_mode != mode:
    mode_name = "バリュー株" if cached_mode == "value" else "成長株"
    st.info(f"表示中のデータは **{mode_name}モード** で実行した結果です。「🚀 パイプライン実行」で現在のモードに更新できます。")
if generated_at:
    try:
        dt = datetime.fromisoformat(generated_at)
        st.caption(f"最終実行: {dt.strftime('%Y-%m-%d %H:%M')}　（リロードしても結果は保持されます）")
    except Exception:
        pass

# ── 地合いフィルター ──────────────────────────────────────────────────
if market:
    state = market.get("state", "")
    level = market.get("investment_level", 1.0)
    comment = market.get("comment", "")
    state_color = {"強気": "#26a69a", "中立": "#ff9800", "弱気": "#ef5350"}.get(state, "#888")
    level_str   = {1.0: "フル投資", 0.5: "投資額50%に抑制", 0.0: "新規買い停止"}.get(level, "")

    st.markdown(
        f'<div style="background:#1e2130;border-left:4px solid {state_color};'
        f'padding:12px 16px;border-radius:6px;margin-bottom:12px">'
        f'<span style="color:{state_color};font-weight:bold;font-size:1.1rem">■ 地合い：{state}</span>'
        f'　<span style="color:#aaa;font-size:0.85rem">{comment}　→　<b>{level_str}</b></span>'
        f'<br><span style="font-size:0.78rem;color:#888">'
        f'TOPIX ETF: {market.get("topix_close","N/A")} (MA25: {market.get("topix_ma25","N/A")}) '
        f'{"▲MA25上" if market.get("topix_above") else "▼MA25下" if market.get("topix_above") is False else "N/A"}'
        f'　｜　グロース250 ETF: {market.get("growth_close","N/A")} (MA25: {market.get("growth_ma25","N/A")}) '
        f'{"▲MA25上" if market.get("growth_above") else "▼MA25下" if market.get("growth_above") is False else "N/A"}'
        f'</span></div>',
        unsafe_allow_html=True,
    )

# ── サマリー指標 ──────────────────────────────────────────────────────
c1, c2, c3, c4 = st.columns(4)
c1.metric("全銘柄", f"{stats['total']:,}")
c2.metric("フィルタ通過", stats["filtered"])
c3.metric("最終候補", stats["top10"])
if top10.empty:
    st.warning("フィルタ通過銘柄がありませんでした。条件を見直してください。")
    st.stop()
c4.metric("最高総合スコア", f"{top10['total_score'].iloc[0]:.1f}")

st.divider()

# ── Claude AI コメント ────────────────────────────────────────────────
if ai and not ai.get("error"):
    comment = ai.get("market_comment", "")
    if comment:
        st.info(f"**市場環境コメント（Claude）**\n\n{comment}")

    top3 = ai.get("top3", [])
    top3_reason = ai.get("top3_reason", "")
    if top3:
        st.success(f"**最有望ベスト3：{' / '.join(top3)}**\n\n{top3_reason}")
elif ai and ai.get("error"):
    st.warning(f"Claude API エラー: {ai['error']}")

# ── グラフ ────────────────────────────────────────────────────────────
col_l, col_r = st.columns(2)
with col_l:
    st.plotly_chart(_chart_score_bar(top10), use_container_width=True)
with col_r:
    st.plotly_chart(_chart_scatter(top10), use_container_width=True)

st.divider()

# ── TOP10 スコアカード ─────────────────────────────────────────────────
ai_stocks = {s["code"]: s for s in ai.get("stocks", [])} if (ai and not ai.get("error")) else {}

tab1, tab2 = st.tabs(["スコア TOP10", "SEPA2絞り込み TOP10"])

with tab1:
    st.subheader("スコアランキング TOP10")
    for rank, row in enumerate(top10.itertuples(), 1):
        _render_scorecard(rank, row, ai_stocks, mode=cached_mode)

with tab2:
    if not scored.empty and "sepa_stage" in scored.columns:
        sepa2_df = (
            scored[scored["sepa_stage"] == 2]
            .sort_values("total_score", ascending=False)
            .head(10)
            .reset_index(drop=True)
        )
    else:
        sepa2_df = pd.DataFrame()

    if sepa2_df.empty:
        st.info("SEPA Stage2条件を満たす銘柄がありませんでした。")
    else:
        sepa2_count = int((scored["sepa_stage"] == 2).sum()) if not scored.empty else 0
        st.subheader(f"SEPA2絞り込み TOP10（全{sepa2_count}件中）")
        st.caption("フィルタ通過銘柄のうち SEPA Stage2 を満たす銘柄をスコア順に表示。")
        for rank, row in enumerate(sepa2_df.itertuples(), 1):
            _render_scorecard(rank, row, ai_stocks, key_prefix="t2", mode=cached_mode)

st.divider()

# ── 全通過銘柄テーブル ─────────────────────────────────────────────────
with st.expander(f"フィルタ通過銘柄 全 {stats['filtered']} 件"):
    disp_cols = [
        "code_4", "company_name", "close", "market_cap",
        "rev_growth", "profit_growth", "ROE", "PER", "PBR",
        "funda_score", "tech_score", "total_score",
    ]
    disp = scored[[c for c in disp_cols if c in scored.columns]].copy()
    if "market_cap" in disp:
        disp["market_cap"] = (disp["market_cap"] / 1e8).round(0).astype("Int64")
    st.dataframe(disp, use_container_width=True)

    csv = disp.to_csv(index=False, encoding="utf-8-sig")
    st.download_button("CSV ダウンロード", csv, "pipeline_result.csv", "text/csv")

st.divider()

# ── claude.ai 用テキスト ──────────────────────────────────────────────
st.subheader("📋 claude.ai 用テキスト")
st.caption("このテキストをコピーして claude.ai のプロジェクトに貼り付けてください。")

def _build_claude_text(top10: pd.DataFrame, ai: dict, market: dict, mode: str = "growth") -> str:
    mode_label  = "バリュー株" if mode == "value" else "成長株"
    score_design = (
        "ファンダ60%（Value50+Quality25+Growth25）＋テクニカル40%（MA/RSI30-55/MACD/出来高/高値ブレイク）"
        if mode == "value" else
        "ファンダ60%（Growth50+Quality25+Value25）＋テクニカル40%（MA/RSI50-65/MACD/出来高/高値ブレイク）"
    )
    analyst_instruction = (
        "バリュー投資の専門家として、割安の根拠・バリュートラップリスク・回復カタリストを分析してください。"
        if mode == "value" else
        "プロのファンドマネージャーとして投資分析をしてください。"
    )
    lines = [
        f"以下はクオンツ＋テクニカルスクリーニングで抽出された{mode_label}TOP10銘柄です。",
        analyst_instruction,
        "",
        f"【スクリーニング日時】{datetime.now().strftime('%Y-%m-%d')}",
        f"【スコア設計】{score_design}",
    ]

    # 地合い情報
    if market:
        state = market.get("state", "N/A")
        level_str = {1.0: "フル投資", 0.5: "投資額50%に抑制", 0.0: "新規買い停止"}.get(
            market.get("investment_level"), "N/A"
        )
        topix_close = market.get("topix_close", "N/A")
        topix_ma25  = market.get("topix_ma25",  "N/A")
        topix_dir   = "▲MA25上" if market.get("topix_above") else "▼MA25下"
        gr_close    = market.get("growth_close", "N/A")
        gr_ma25     = market.get("growth_ma25",  "N/A")
        gr_dir      = "▲MA25上" if market.get("growth_above") else "▼MA25下"
        lines += [
            "",
            f"【地合い】{state}（{level_str}）",
            f"TOPIX ETF: {topix_close} (MA25:{topix_ma25}) {topix_dir}"
            f" / グロース250 ETF: {gr_close} (MA25:{gr_ma25}) {gr_dir}",
        ]

    lines += [
        "",
        "| # | コード | 会社名 | セクター | 株価 | 売上成長% | 利益成長% | ROE% | PER | ファンダ | テクニカル | 総合 | シグナル | エントリー | 損切り | 利確 |",
        "|---|--------|--------|--------|------|---------|---------|------|-----|--------|----------|------|---------|---------|------|------|",
    ]
    for i, r in enumerate(top10.itertuples(), 1):
        sig    = getattr(r, "signal", "") or ""
        eb     = getattr(r, "entry_breakout", None)
        stop   = getattr(r, "stop_loss", None)
        stopp  = getattr(r, "stop_pct", None)
        tgt    = getattr(r, "target", None)
        eb_str   = f"¥{eb:,.0f}"   if eb   and pd.notna(eb)   else "-"
        stop_str = f"¥{stop:,.0f}({stopp:.1f}%)" if stop and pd.notna(stop) and stopp and pd.notna(stopp) else "-"
        tgt_str  = f"¥{tgt:,.0f}"  if tgt  and pd.notna(tgt)  else "-"
        lines.append(
            f"| {i} | {r.code_4} | {r.company_name} | {r.sector} "
            f"| ¥{r.close:,.0f} | {r.rev_growth:.1f} | {r.profit_growth:.1f} "
            f"| {r.ROE:.1f} | {r.PER:.1f} "
            f"| {r.funda_score:.1f} | {r.tech_score:.1f} | {r.total_score:.1f} "
            f"| {sig} | {eb_str} | {stop_str} | {tgt_str} |"
        )

    lines += [
        "",
        "【各銘柄のテクニカル情報】",
    ]
    for i, r in enumerate(top10.itertuples(), 1):
        detail = r.tech_detail if isinstance(r.tech_detail, dict) else {}
        rsi   = detail.get("rsi",      "N/A")
        ma25  = detail.get("ma25",     "N/A")
        ma60  = detail.get("ma60",     "N/A")
        ma200 = detail.get("ma200",    "N/A")
        vr    = detail.get("vol_ratio", "N/A")
        div_t = int(getattr(r, "div_trend", 0) or 0)
        div_str = {0: "なし", 1: "増配", 2: "2期連続増配"}.get(div_t, "N/A")
        base_line = (
            f"{i}. {r.code_4} {r.company_name}："
            f"RSI={rsi} / MA25={ma25} / MA60={ma60} / 出来高比率={vr}"
        )
        if mode == "value":
            ma200_dev_v  = detail.get("ma200_dev",    None)
            vol_trend_rv = detail.get("vol_trend_ratio", None)
            dev_str2     = f"({ma200_dev_v:+.1f}%)" if ma200_dev_v is not None else ""
            op_t         = int(getattr(r, "op_trend",      0)     or 0)
            op_turn      = bool(getattr(r, "op_turnaround", False) or False)
            pr           = getattr(r, "payout_ratio", None)
            op_str       = "V字転換" if op_turn else {0: "横ばい/減", 1: "増益", 2: "2期連続増益"}.get(op_t, "N/A")
            pr_str       = f"{pr:.0f}%" if pr is not None and not (isinstance(pr, float) and np.isnan(pr)) else "N/A"
            vt_str       = f"{vol_trend_rv:.2f}x" if vol_trend_rv is not None else "N/A"
            base_line   += f" / MA200={ma200}{dev_str2} / 営業益={op_str} / 増配={div_str} / 配当性向={pr_str} / 需給={vt_str}"
        lines.append(base_line)

    lines += [
        "",
        "【各銘柄のトレンド・財務健全性指標】",
    ]
    for i, r in enumerate(top10.itertuples(), 1):
        sepa_stage  = getattr(r, "sepa_stage",  0)
        sepa_rs     = getattr(r, "sepa_rs",     None)
        altman_z    = getattr(r, "altman_z",    None)
        gran_g1     = getattr(r, "gran_g1",     False)
        gran_g2     = getattr(r, "gran_g2",     False)
        dow_up      = getattr(r, "dow_uptrend", False)

        stage_label = {1: "Stage1(基盤形成)", 2: "Stage2(上昇)", 3: "Stage3(天井)", 4: "Stage4(下降)"}.get(sepa_stage, f"Stage{sepa_stage}")
        rs_str   = f"{sepa_rs:+.1f}%" if sepa_rs is not None and pd.notna(sepa_rs) else "N/A"
        az_str   = f"{altman_z:.2f}" if altman_z is not None and pd.notna(altman_z) else "N/A"
        gran_str = "/".join(filter(None, ["G1" if gran_g1 else "", "G2" if gran_g2 else ""])) or "なし"
        dow_str  = "上昇トレンド" if dow_up else "非上昇"

        lines.append(
            f"{i}. {r.code_4} {r.company_name}："
            f"SEPA={stage_label} / 対TOPIX RS={rs_str} / "
            f"AltmanZ={az_str} / グランビル={gran_str} / ダウ理論={dow_str}"
        )

    # アプリ分析結果があれば参考として添付
    if ai and not ai.get("error") and ai.get("stocks"):
        lines += [
            "",
            "【参考：アプリ側のClaude分析結果（比較用）】",
        ]
        ai_map = {s["code"]: s for s in ai.get("stocks", [])}
        for i, r in enumerate(top10.itertuples(), 1):
            s = ai_map.get(r.code_4, {})
            if s:
                lines.append(
                    f"{i}. {r.code_4} {r.company_name} "
                    f"→ {s.get('judgment','')} / 上昇余地:{s.get('upside','')} / "
                    f"ストーリー:{s.get('story','')}"
                )

    if mode == "value":
        lines += [
            "",
            "上記データをもとに：",
            "① 各銘柄の割安の根拠・財務的強み・バリュートラップリスク・回復カタリストを分析してください",
            "② 最も有望なバリュー株ベスト3を選んでその理由を教えてください（PBR改善余地・株主還元・業績回復を重視）",
            "③ 今の市場環境（2026年4月）を踏まえたバリュー投資戦略コメントをお願いします",
        ]
    else:
        lines += [
            "",
            "上記データをもとに：",
            "① 各銘柄の投資ストーリー・強み・リスク・カタリストを分析してください",
            "② 最も有望な銘柄ベスト3を選んでその理由を教えてください",
            "③ 今の市場環境（2026年4月）を踏まえた投資戦略コメントをお願いします",
        ]
    return "\n".join(lines)

claude_text = _build_claude_text(top10, ai, market, mode=cached_mode)
st.text_area(
    label="テキストをコピーしてclaude.aiに貼り付け",
    value=claude_text,
    height=300,
    label_visibility="collapsed",
)
st.caption(f"文字数: {len(claude_text):,}文字")

st.divider()

# ── HTML レポート保存 ─────────────────────────────────────────────────
st.subheader("レポート出力")
col_save, col_dl = st.columns(2)

with col_save:
    if st.button("💾 HTMLをDropboxに保存", use_container_width=True):
        try:
            html_content = _generate_html(top10, ai or {}, stats, mode=cached_mode)
            saved_path   = _save_html(html_content)
            st.success(f"保存しました: {saved_path}")
        except Exception as e:
            st.error(f"保存エラー: {e}")

with col_dl:
    try:
        html_content = _generate_html(top10, ai or {}, stats, mode=cached_mode)
        st.download_button(
            "⬇ HTML ダウンロード",
            html_content,
            f"pipeline_{datetime.now().strftime('%Y%m%d')}.html",
            "text/html",
            use_container_width=True,
        )
    except Exception as e:
        st.error(f"HTML生成エラー: {e}")

# ── API コスト表示 ────────────────────────────────────────────────────
if ai and not ai.get("error") and "cost_usd" in ai:
    cost_jpy = ai["cost_usd"] * 150
    st.caption(
        f"Claude API コスト: ${ai['cost_usd']:.4f} (約 ¥{cost_jpy:.0f}) ／ "
        f"入力 {ai.get('input_tokens', 0):,} tok ／ 出力 {ai.get('output_tokens', 0):,} tok"
    )
