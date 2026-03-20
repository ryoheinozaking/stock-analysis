# -*- coding: utf-8 -*-
"""
ポートフォリオAI分析ページ
Claude API を使って保有銘柄のファンダ・テクニカル両面から売買提案を行う
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))
import json
import glob

import streamlit as st

st.set_page_config(layout="wide")
import pandas as pd

from services.batch_service import load_cache, get_cache_updated_at
from services.jquants_service import get_listed_info
from services.portfolio_service import parse_sbi_csv
from services.claude_service import (
    build_stock_context, fetch_fallback_metrics,
    analyze_portfolio, parse_action_label, calc_cost,
)

ANALYSIS_SAVE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "ai_analysis.json")
DOWNLOADS_DIR = os.path.expanduser("~/Downloads")
def _find_latest_sbi_csv():
    candidates = glob.glob(os.path.join(DOWNLOADS_DIR, "New_file*.csv"))
    if candidates:
        return max(candidates, key=os.path.getmtime)
    candidates = glob.glob(os.path.join(DOWNLOADS_DIR, "*.csv"))
    return max(candidates, key=os.path.getmtime) if candidates else None
def _auto_load_portfolio():
    path = _find_latest_sbi_csv()
    if path:
        try:
            with open(path, "rb") as f:
                stocks_df, funds_df = parse_sbi_csv(f)
            if not stocks_df.empty:
                st.session_state["portfolio_df"] = stocks_df
                st.session_state["portfolio_updated"] = os.path.getmtime(path)
            if not funds_df.empty:
                st.session_state["funds_df"] = funds_df
        except Exception:
            pass
def _save_analysis(result, contexts):
    os.makedirs(os.path.dirname(ANALYSIS_SAVE_PATH), exist_ok=True)
    with open(ANALYSIS_SAVE_PATH, "w", encoding="utf-8") as f:
        json.dump({"result": result, "contexts": contexts}, f, ensure_ascii=False, indent=2)
def _load_analysis():
    if os.path.exists(ANALYSIS_SAVE_PATH):
        try:
            with open(ANALYSIS_SAVE_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data.get("result"), data.get("contexts")
        except Exception:
            pass
    return None, None

# カスタムCSS読み込み
css_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "styles", "custom.css")
if os.path.exists(css_path):
    with open(css_path, "r", encoding="utf-8") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

st.title("🤖 ポートフォリオAI分析")

# session_state 初期化
if "ai_analysis" not in st.session_state:
    st.session_state["ai_analysis"] = None
if "_ai_contexts" not in st.session_state:
    st.session_state["_ai_contexts"] = None

# ポートフォリオ自動読み込み
if st.session_state.get("portfolio_df") is None:
    _auto_load_portfolio()

# 分析結果をファイルから復元
if st.session_state["ai_analysis"] is None:
    saved_result, saved_contexts = _load_analysis()
    if saved_result:
        st.session_state["ai_analysis"] = saved_result
        st.session_state["_ai_contexts"] = saved_contexts

# ---- サイドバー ----
with st.sidebar:
    st.header("💹 ポートフォリオデータ")
    if st.session_state.get("portfolio_df") is not None:
        updated = st.session_state.get("portfolio_updated", "不明")
        count   = len(st.session_state["portfolio_df"])
        st.success(f"読み込み済み: {count} 銘柄（{updated}）")
    else:
        st.warning("未読み込み")
    st.page_link("pages/4_portfolio.py", label="CSVを読み込む / 更新する", icon="💹")

    st.divider()
    cache_updated = get_cache_updated_at()
    if cache_updated:
        st.caption(f"📦 スクリーニングキャッシュ: {cache_updated.strftime('%Y/%m/%d %H:%M')}")
    else:
        st.warning("キャッシュなし（スクリーニングページでデータ更新してください）")

# ---- ポートフォリオ未読み込み時の誘導 ----
if st.session_state.get("portfolio_df") is None:
    st.info("**使い方:** まず「💹 ポートフォリオ」ページでSBI証券のCSVを読み込んでから、このページに戻ってください。")
    if st.button("💹 ポートフォリオページへ", type="primary"):
        st.switch_page("pages/4_portfolio.py")
    st.stop()

# ---- データ準備 ----
df       = st.session_state["portfolio_df"].copy()
cache_df = load_cache()
listed_df = get_listed_info()

# セクター情報をdfに付与（まだない場合）
if "セクター" not in df.columns and not listed_df.empty:
    listed_df["code_4"] = listed_df["Code"].str[:4]
    sector_map = listed_df.drop_duplicates("code_4").set_index("code_4")[["S33Nm", "MktNm"]].to_dict("index")
    df["セクター"] = df["code_4"].map(lambda c: sector_map.get(c, {}).get("S33Nm", "不明"))

stock_contexts = build_stock_context(df, cache_df, listed_df)

# キャッシュ未収録銘柄を確認
missing_codes = [
    ctx["code"] for ctx in stock_contexts
    if ctx.get("per") is None and ctx.get("fund_score") is None
]

# ---- 送信データプレビュー ----
with st.expander("📋 送信データプレビュー", expanded=False):
    preview_rows = []
    for ctx in stock_contexts:
        preview_rows.append({
            "コード": ctx["code"],
            "会社名": ctx["name"],
            "セクター": ctx["sector"],
            "評価額(万円)": round(ctx["market_value"] / 10000, 1),
            "損益(%)": ctx["pnl_pct"],
            "PER": ctx["per"],
            "PBR": ctx["pbr"],
            "ROE": ctx["roe"],
            "RSI": ctx["rsi"],
            "シグナル": ctx["signals"] or "−",
            "ファンダスコア": ctx["fund_score"],
        })
    st.dataframe(
        pd.DataFrame(preview_rows),
        use_container_width=True,
        hide_index=True,
        column_config={
            "損益(%)": st.column_config.NumberColumn(format="%+.2f%%"),
        }
    )
    if missing_codes:
        st.warning(f"キャッシュ未収録の銘柄 {len(missing_codes)} 件は実行時にJ-Quantsから取得します: {', '.join(missing_codes)}")

# ---- コスト目安 ----
n = len(stock_contexts)
est_input  = 600 + n * 250
est_output = 2000
est_cost   = calc_cost(est_input, est_output)
st.caption(f"推定コスト: 約 ${est_cost:.4f} USD（入力 ~{est_input:,} トークン / 出力 ~{est_output:,} トークン）")

# ---- 実行ボタン ----
col_run, col_clear = st.columns([3, 1])
with col_run:
    run_button = st.button("🤖 AI分析を実行", type="primary", use_container_width=True)
with col_clear:
    if st.session_state["ai_analysis"] and st.button("🔄 再分析", use_container_width=True):
        st.session_state["ai_analysis"] = None
        st.rerun()

# ---- AI分析実行 ----
if run_button:
    # フォールバック取得
    if missing_codes:
        with st.spinner(f"J-Quantsから {len(missing_codes)} 銘柄のデータを取得中..."):
            for ctx in stock_contexts:
                if ctx["code"] in missing_codes:
                    fallback = fetch_fallback_metrics(ctx["code"])
                    if fallback:
                        ctx.update(fallback)

    # ポートフォリオサマリー
    portfolio_summary = {
        "total_value":   float(df["評価額"].sum()),
        "total_pnl":     float(df["損益"].sum()),
        "total_pnl_pct": float(df["損益"].sum() / df["取得総額"].sum() * 100)
                         if df["取得総額"].sum() > 0 else 0.0,
        "count": len(df),
        "sectors": df["セクター"].value_counts().to_dict() if "セクター" in df.columns else {},
    }

    with st.spinner("🤖 Claudeが分析中... (10〜30秒かかります)"):
        try:
            result = analyze_portfolio(stock_contexts, portfolio_summary)
            st.session_state["ai_analysis"] = result
            st.session_state["_ai_contexts"] = stock_contexts
            _save_analysis(result, stock_contexts)
        except ValueError as e:
            st.error(f"分析結果の解析に失敗しました: {e}")
        except Exception as e:
            st.error(f"AI分析でエラーが発生しました: {e}")

# ---- 分析結果表示 ----
if st.session_state.get("ai_analysis"):
    result       = st.session_state["ai_analysis"]
    contexts     = st.session_state.get("_ai_contexts", stock_contexts)
    overall      = result.get("overall", {})
    stocks_eval  = result.get("stocks", [])
    actions      = result.get("actions", [])

    st.markdown("---")

    # --- 全体総評 ---
    st.markdown("## 📊 ポートフォリオ全体総評")

    risk_color = {"低": "#26a69a", "中": "#ff9800", "高": "#ef5350"}.get(
        overall.get("risk_level", "中"), "#ff9800"
    )
    st.markdown(f"""
    <div style="background:#1a1f2e; border-radius:12px; padding:20px;
                border-left:4px solid {risk_color}; margin-bottom:16px;">
        <span style="color:{risk_color}; font-weight:bold; font-size:1rem;">
            リスク水準: {overall.get("risk_level", "不明")}
        </span>
        <p style="color:#e8eaf0; margin:12px 0 0 0; font-size:0.97rem; line-height:1.8;">
            {overall.get("summary", "")}
        </p>
    </div>
    """, unsafe_allow_html=True)

    col_s, col_w = st.columns(2)
    with col_s:
        st.markdown("**強み**")
        for s in overall.get("strengths", []):
            st.markdown(f"- {s}")
    with col_w:
        st.markdown("**弱み**")
        for w in overall.get("weaknesses", []):
            st.markdown(f"- {w}")

    if overall.get("sector_bias"):
        st.caption(f"セクター偏り: {overall['sector_bias']}")

    # --- 銘柄別評価 ---
    st.markdown("---")
    st.markdown("## 🏷️ 銘柄別評価")

    eval_map = {s["code"]: s for s in stocks_eval}
    rows = []
    for ctx in contexts:
        code = ctx["code"]
        ev   = eval_map.get(code, {})
        action = ev.get("action", "継続保有")
        emoji, color = parse_action_label(action)
        rows.append({
            "コード":   code,
            "会社名":   ctx.get("name", ""),
            "セクター": ctx.get("sector", ""),
            "評価額(万)": round(ctx.get("market_value", 0) / 10000, 1),
            "損益(%)":  ctx.get("pnl_pct", 0),
            "判定":     f"{emoji} {action}",
            "_color":   color,
            "理由":     ev.get("reason", ""),
            "注目点":   ev.get("focus", ""),
        })

    eval_df = pd.DataFrame(rows)

    # 判定列に色付け
    def _color_action(val):
        colors = {
            "買い増し推奨": "color:#26a69a; font-weight:bold",
            "継続保有":     "color:#2196f3",
            "利確検討":     "color:#ff9800; font-weight:bold",
            "損切り検討":   "color:#ef5350; font-weight:bold",
        }
        for k, v in colors.items():
            if k in str(val):
                return v
        return ""

    # 概要テーブル（理由・注目点は除く）
    summary_df = eval_df.drop(columns=["_color", "理由", "注目点"])
    st.dataframe(
        summary_df.style.map(_color_action, subset=["判定"]),
        use_container_width=True,
        hide_index=True,
        column_config={
            "損益(%)":    st.column_config.NumberColumn(format="%+.2f%%"),
            "評価額(万)": st.column_config.NumberColumn(format="%.1f万"),
        }
    )

    # 銘柄別詳細（理由・注目点を全文表示）
    st.markdown("**銘柄別詳細**")
    for _, row in eval_df.iterrows():
        emoji, color = parse_action_label(row["判定"].split(" ", 1)[-1] if " " in row["判定"] else row["判定"])
        with st.expander(f"{row['コード']} {row['会社名']}　{row['判定']}"):
            st.markdown(f"**理由:** {row['理由']}")
            st.markdown(f"**注目点:** {row['注目点']}")

    # 銘柄詳細ページへのリンク
    selected_code = st.selectbox(
        "銘柄を選んで詳細チャートを見る",
        options=[""] + [f"{r['コード']} {r['会社名']}" for _, r in eval_df.iterrows()],
        index=0,
    )
    if selected_code:
        code_4 = selected_code[:4]
        if st.button("📈 詳細チャートを見る", type="primary"):
            st.session_state["selected_code"] = code_4 + "0"
            st.switch_page("pages/2_stock_detail.py")

    # --- アクション提案 ---
    st.markdown("---")
    st.markdown("## ✅ 今すぐやること")
    for i, action_item in enumerate(actions, 1):
        st.markdown(f"**{i}.** {action_item}")

    # --- コスト表示 ---
    st.markdown("---")
    input_tok  = result.get("input_tokens", 0)
    output_tok = result.get("output_tokens", 0)
    cost_usd   = calc_cost(input_tok, output_tok)
    cost_jpy   = cost_usd * 150  # 概算
    st.caption(
        f"使用トークン: 入力 {input_tok:,} / 出力 {output_tok:,}  |  "
        f"推定コスト: ${cost_usd:.4f} USD（約{cost_jpy:.1f}円）"
    )
