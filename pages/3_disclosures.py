# -*- coding: utf-8 -*-
"""
適時開示ページ（一覧ブラウザ + AI要約フィルタ + 要約済み一覧）
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))
import json
from datetime import datetime, timedelta

import streamlit as st

st.set_page_config(layout="wide")
import pandas as pd

from services.tdnet_service import get_latest, get_by_date, get_by_company
from components.disclosure_table import render_disclosure_table
from services.ir_service import (
    get_portfolio_codes,
    get_screening_top_codes,
    classify_disclosures,
    fetch_pdf_text,
    get_disclosures_by_date_range,
)
from services.claude_service import summarize_ir, calc_cost, IMPACT_COLOR

# カスタムCSS
css_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "styles", "custom.css")
if os.path.exists(css_path):
    with open(css_path, "r", encoding="utf-8") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

SUMMARY_CACHE_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "ir_summaries.json"
)

LAYER_LABELS = {
    1: ("🔴 必読", "#ef5350"),
    2: ("🟡 推奨", "#ff9800"),
    3: ("🔵 参考", "#2196f3"),
}

IMPACT_EMOJI = {
    "ポジティブ": "📈",
    "ニュートラル": "➡️",
    "ネガティブ": "📉",
}

# ---- キャッシュI/O ----

def _load_summary_cache() -> dict:
    if os.path.exists(SUMMARY_CACHE_PATH):
        try:
            with open(SUMMARY_CACHE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}
def _save_summary_cache(cache: dict):
    os.makedirs(os.path.dirname(SUMMARY_CACHE_PATH), exist_ok=True)
    with open(SUMMARY_CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)
# ---- 初期化 ----

if "ir_summary_cache" not in st.session_state:
    st.session_state["ir_summary_cache"] = _load_summary_cache()

if "ir_df" not in st.session_state:
    st.session_state["ir_df"] = pd.DataFrame()

if "ir_total_cost" not in st.session_state:
    cache = st.session_state["ir_summary_cache"]
    st.session_state["ir_total_cost"] = sum(
        calc_cost(v.get("input_tokens", 0), v.get("output_tokens", 0))
        for v in cache.values()
        if isinstance(v, dict)
    )

# ---- ページ ----

st.title("📰 適時開示")

tab1, tab2, tab3 = st.tabs(["📋 一覧ブラウザ", "✨ AI要約フィルタ", "📑 要約済み一覧"])
# =========================================================
# Tab 1: 一覧ブラウザ
# =========================================================
with tab1:
    col_ctrl, col_main = st.columns([1, 3])

    with col_ctrl:
        st.markdown("#### 検索条件")
        mode = st.radio(
            "表示モード",
            options=["最新一覧", "日付指定", "銘柄コード検索"],
            index=0,
        )

        limit_val = 50
        date_str = None
        company_code_input = None

        if mode == "最新一覧":
            limit_val = st.slider("表示件数", 10, 200, 50, step=10)

        elif mode == "日付指定":
            today = datetime.today().date()
            selected_date = st.date_input(
                "日付を選択",
                value=today,
                min_value=today - timedelta(days=30),
                max_value=today,
            )
            date_str = selected_date.strftime("%Y%m%d")
            limit_val = st.slider("表示件数", 10, 200, 100, step=10)

        elif mode == "銘柄コード検索":
            company_code_input = st.text_input(
                "4桁の銘柄コード",
                placeholder="例: 7203",
                max_chars=4,
            )
            limit_val = st.slider("表示件数上限", 10, 100, 50, step=10)

        st.button("🔄 更新", type="primary", use_container_width=True, key="tab1_refresh")

    with col_main:
        df = None

        if mode == "最新一覧":
            st.markdown(f"### 最新の適時開示（最大 {limit_val} 件）")
            with st.spinner("取得中..."):
                df = get_latest(limit=limit_val)

        elif mode == "日付指定":
            if date_str:
                st.markdown(f"### {selected_date.strftime('%Y年%m月%d日')} の適時開示")
                with st.spinner("取得中..."):
                    df = get_by_date(date_str=date_str, limit=limit_val)
            else:
                st.info("日付を選択してください")

        elif mode == "銘柄コード検索":
            if company_code_input and len(company_code_input) == 4:
                st.markdown(f"### 銘柄コード `{company_code_input}` の適時開示（過去30日）")
                with st.spinner("取得中..."):
                    df = get_by_company(code_4digit=company_code_input, days=30, limit=limit_val)
            elif company_code_input and len(company_code_input) != 4:
                st.warning("銘柄コードは4桁で入力してください")
            else:
                st.info("4桁の銘柄コードを入力してください")

        if df is not None:
            if df.empty:
                st.warning("該当する開示情報がありません。")
            else:
                st.markdown(f"**{len(df)} 件**")
                render_disclosure_table(df)
                csv_data = df.to_csv(index=False, encoding="utf-8-sig")
                st.download_button(
                    label="📥 CSVダウンロード",
                    data=csv_data,
                    file_name=f"disclosures_{datetime.today().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                )
# =========================================================
# Tab 2: AI要約フィルタ
# =========================================================
with tab2:
    col_ctrl2, col_main2 = st.columns([1, 3])

    with col_ctrl2:
        st.markdown("#### フィルタ設定")

        today2 = datetime.today().date()
        start_date = st.date_input(
            "開始日",
            value=today2 - timedelta(days=6),
            min_value=today2 - timedelta(days=60),
            max_value=today2,
            key="ir_start_date",
        )
        end_date = st.date_input(
            "終了日",
            value=today2,
            min_value=today2 - timedelta(days=60),
            max_value=today2,
            key="ir_end_date",
        )

        if start_date > end_date:
            st.warning("開始日 ≤ 終了日にしてください")

        screening_top_n = st.slider("スクリーニング上位（層2）", 10, 100, 50, step=10)
        fetch_limit2 = st.slider("1日あたり取得上限", 50, 300, 150, step=50)

        days_range = (end_date - start_date).days + 1
        st.caption(f"対象: {days_range}日分")

        load_btn = st.button("🔄 取得・フィルタリング", type="primary", use_container_width=True)

        st.markdown("---")
        st.caption("**凡例**")
        st.markdown("🔴 **必読**: 保有銘柄の全開示")
        st.markdown("🟡 **推奨**: スクリーニング上位×決算関連（上下方修正含む）")
        st.markdown("🔵 **参考**: 上方修正／増配／黒字転換（ポジティブのみ）")
        st.markdown("---")
        st.metric("累計コスト", f"${st.session_state['ir_total_cost']:.4f}")

    with col_main2:
        if load_btn:
            if start_date > end_date:
                st.error("開始日・終了日を確認してください")
            else:
                with st.spinner(f"{days_range}日分の開示を取得中..."):
                    raw_df = get_disclosures_by_date_range(start_date, end_date, limit_per_day=fetch_limit2)

                if raw_df.empty:
                    st.warning("該当期間の開示情報が見つかりませんでした。")
                else:
                    with st.spinner("フィルタリング中..."):
                        port_codes = get_portfolio_codes()
                        screen_codes = get_screening_top_codes(n=screening_top_n)
                        filtered_df = classify_disclosures(raw_df, port_codes, screen_codes)

                    st.session_state["ir_df"] = filtered_df
                    st.session_state["ir_port_codes"] = port_codes
                    st.session_state["ir_screen_codes"] = screen_codes

        filtered_df = st.session_state.get("ir_df", pd.DataFrame())

        if filtered_df.empty:
            st.info("「🔄 取得・フィルタリング」をクリックしてください。")
        else:
            summary_cache = st.session_state["ir_summary_cache"]
            st.markdown(f"**重要開示: {len(filtered_df)} 件**（絞り込み済み）")

            for layer_id in [1, 2, 3]:
                layer_df = filtered_df[filtered_df["layer"] == layer_id]
                if layer_df.empty:
                    continue

                label, color = LAYER_LABELS[layer_id]
                st.markdown(
                    f"### {label}　<span style='color:{color}; font-size:0.85em;'>（{len(layer_df)}件）</span>",
                    unsafe_allow_html=True,
                )

                for _, row in layer_df.iterrows():
                    doc_id = str(row.get("id", ""))
                    title = str(row.get("title", ""))
                    company = str(row.get("company_name", ""))
                    code_4 = str(row.get("company_code", ""))[:4]
                    pubdate = str(row.get("pubdate", ""))
                    doc_url = str(row.get("document_url", ""))
                    layer = int(row.get("layer", 0))

                    with st.container(border=True):
                        hcol1, hcol2 = st.columns([6, 2])
                        with hcol1:
                            st.markdown(
                                f"**{company}**（{code_4}）　"
                                f"<span style='color:#aaa; font-size:0.85em;'>{pubdate}</span>",
                                unsafe_allow_html=True,
                            )
                            st.markdown(f"📄 {title}")
                            if doc_url:
                                st.markdown(f"[開示原文を見る]({doc_url})")
                        with hcol2:
                            summarize_btn = st.button(
                                "✨ AI要約",
                                key=f"summarize_{doc_id}",
                                use_container_width=True,
                                type="secondary",
                            )

                        cached = summary_cache.get(doc_id)
                        if summarize_btn and not cached:
                            with st.spinner("要約中..."):
                                try:
                                    pdf_text = fetch_pdf_text(doc_url) if doc_url else ""
                                    result = summarize_ir(title, company, pdf_text)
                                    # メタデータも一緒に保存
                                    result["_meta"] = {
                                        "title": title,
                                        "company_name": company,
                                        "company_code": code_4,
                                        "pubdate": pubdate,
                                        "doc_url": doc_url,
                                        "layer": layer,
                                        "saved_at": datetime.today().strftime("%Y-%m-%d"),
                                    }
                                    summary_cache[doc_id] = result
                                    st.session_state["ir_summary_cache"] = summary_cache
                                    _save_summary_cache(summary_cache)
                                    cost = calc_cost(result["input_tokens"], result["output_tokens"])
                                    st.session_state["ir_total_cost"] += cost
                                    cached = result
                                except Exception as e:
                                    st.error(f"要約失敗: {e}")

                        if cached:
                            impact = cached.get("impact", "")
                            impact_color = IMPACT_COLOR.get(impact, "#7a8499")
                            st.markdown(
                                f"**{cached.get('headline', '')}**　"
                                f"<span style='background:{impact_color}; color:white; "
                                f"padding:2px 8px; border-radius:4px; font-size:0.85em;'>{impact}</span>",
                                unsafe_allow_html=True,
                            )
                            st.markdown(cached.get("summary", ""))
                            nums = cached.get("key_numbers", [])
                            if nums:
                                st.markdown(
                                    " &nbsp; ".join([
                                        f"<span style='background:#1e3a5f; color:#7dd3fc; "
                                        f"padding:4px 12px; border-radius:4px; font-size:1em;'>{n}</span>"
                                        for n in nums
                                    ]),
                                    unsafe_allow_html=True,
                                )
                            st.caption(f"💡 {cached.get('investor_action', '')}")

                st.divider()
# =========================================================
# Tab 3: 要約済み一覧
# =========================================================
with tab3:
    summary_cache = st.session_state["ir_summary_cache"]

    if not summary_cache:
        st.info("まだ要約した開示はありません。「✨ AI要約フィルタ」タブで要約してください。")
    else:
        # メタデータがある要約のみ抽出
        entries = []
        for doc_id, v in summary_cache.items():
            if not isinstance(v, dict):
                continue
            meta = v.get("_meta", {})
            entries.append({
                "doc_id": doc_id,
                "pubdate": meta.get("pubdate", ""),
                "company_name": meta.get("company_name", "（不明）"),
                "company_code": meta.get("company_code", ""),
                "title": meta.get("title", ""),
                "doc_url": meta.get("doc_url", ""),
                "layer": meta.get("layer", 0),
                "saved_at": meta.get("saved_at", ""),
                "impact": v.get("impact", ""),
                "headline": v.get("headline", ""),
                "summary": v.get("summary", ""),
                "key_numbers": v.get("key_numbers", []),
                "investor_action": v.get("investor_action", ""),
                "input_tokens": v.get("input_tokens", 0),
                "output_tokens": v.get("output_tokens", 0),
            })

        # 開示日時降順ソート
        entries.sort(key=lambda x: x["pubdate"], reverse=True)

        # フィルタ
        fcol1, fcol2, fcol3 = st.columns([2, 2, 1])
        with fcol1:
            search_word = st.text_input("🔍 会社名・タイトルで検索", placeholder="例: リベラウェア")
        with fcol2:
            impact_filter = st.selectbox(
                "影響度フィルタ",
                options=["すべて", "ポジティブ", "ニュートラル", "ネガティブ"],
            )
        with fcol3:
            st.metric("要約件数", len(entries))

        # フィルタ適用
        if search_word:
            entries = [e for e in entries if search_word in e["company_name"] or search_word in e["title"]]
        if impact_filter != "すべて":
            entries = [e for e in entries if e["impact"] == impact_filter]

        if not entries:
            st.warning("該当する要約がありません。")
        else:
            for e in entries:
                impact_color = IMPACT_COLOR.get(e["impact"], "#7a8499")
                impact_emoji = IMPACT_EMOJI.get(e["impact"], "➡️")
                layer_label = LAYER_LABELS.get(e["layer"], ("", "#999"))[0] if e["layer"] else ""

                with st.container(border=True):
                    hcol1, hcol2 = st.columns([5, 1])
                    with hcol1:
                        st.markdown(
                            f"**{e['company_name']}**（{e['company_code']}）　"
                            f"<span style='color:#aaa; font-size:0.85em;'>{e['pubdate']}</span>　"
                            f"<span style='color:#888; font-size:0.8em;'>{layer_label}</span>",
                            unsafe_allow_html=True,
                        )
                        st.markdown(f"📄 {e['title']}")
                        if e["doc_url"]:
                            st.markdown(f"[開示原文を見る]({e['doc_url']})")
                    with hcol2:
                        st.markdown(
                            f"<div style='text-align:center; background:{impact_color}; color:white; "
                            f"padding:6px; border-radius:6px; font-size:0.9em;'>"
                            f"{impact_emoji}<br>{e['impact']}</div>",
                            unsafe_allow_html=True,
                        )

                    st.markdown(
                        f"**{e['headline']}**"
                    )
                    st.markdown(e["summary"])
                    nums = e.get("key_numbers", [])
                    if nums:
                        st.markdown(
                            " &nbsp; ".join([
                                f"<span style='background:#1e3a5f; color:#7dd3fc; "
                                f"padding:4px 12px; border-radius:4px; font-size:1em;'>{n}</span>"
                                for n in nums
                            ]),
                            unsafe_allow_html=True,
                        )
                    st.caption(f"💡 {e['investor_action']}")
                    cost = calc_cost(e["input_tokens"], e["output_tokens"])
                    st.caption(f"要約日: {e['saved_at']}　コスト: ${cost:.4f}")
