# -*- coding: utf-8 -*-
"""
スクリーニングページ（ファンダ×テクニカル・モメンタム戦略 統合版）
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))

import streamlit as st
st.set_page_config(layout="wide")

import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

# =========================================================
# ウォッチリスト ヘルパー
# =========================================================
_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_WATCHLIST_PATH = Path(_BASE) / "data" / "watchlist.json"

def _load_watchlist():
    if _WATCHLIST_PATH.exists():
        try:
            return json.loads(_WATCHLIST_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return []

def _save_watchlist(items):
    _WATCHLIST_PATH.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")

def _add_to_watchlist(code4, name, target_price):
    items = _load_watchlist()
    if any(i["code"] == code4 for i in items):
        return False
    items.append({
        "code"        : code4,
        "name"        : name,
        "target_price": target_price,
        "memo"        : "",
        "added_at"    : datetime.today().strftime("%Y-%m-%d"),
    })
    _save_watchlist(items)
    return True

# =========================================================
# サービスインポート
# =========================================================
from services.batch_service import (
    load_cache, get_cache_updated_at, fetch_all_stocks,
    PRICES_PATH,
)

# CSS
css_path = os.path.join(_BASE, "styles", "custom.css")
if os.path.exists(css_path):
    st.markdown(f"<style>{open(css_path, encoding='utf-8').read()}</style>", unsafe_allow_html=True)

st.title("⚡ スクリーニング")

# session_state
if "selected_code" not in st.session_state:
    st.session_state["selected_code"] = ""

# =========================================================
# サイドバー
# =========================================================
# ── キャッシュ状態 ──
cache_df      = load_cache()
cache_updated = get_cache_updated_at()
if cache_updated:
    st.sidebar.success(f"📦 キャッシュ: {cache_updated.strftime('%Y/%m/%d %H:%M')} 更新")
else:
    st.sidebar.warning("📦 キャッシュなし（データ更新が必要です）")

update_button = st.sidebar.button("🔄 データ更新", use_container_width=True)

# ── 市場・表示件数 ──
market_options = {
    "東証プライム (0111)" : "0111",
    "東証スタンダード (0112)": "0112",
    "東証グロース (0113)"  : "0113",
}
selected_markets = st.sidebar.multiselect(
    "市場選択",
    options=list(market_options.keys()),
    default=["東証プライム (0111)"],
)
market_codes = [market_options[m] for m in selected_markets]
display_top_n_funda = st.sidebar.slider("表示上位件数", 50, 500, 200, 50)

# ── ファンダフィルター（チェックで有効）──
use_per = st.sidebar.checkbox("PER上限", value=True)
per_max = st.sidebar.slider("PER上限", 5, 50, 20, 1, key="per_max", label_visibility="collapsed") if use_per else None

use_pbr = st.sidebar.checkbox("PBR範囲", value=True)
pbr_range = st.sidebar.slider("PBR範囲", 0.0, 5.0, (0.5, 1.5), 0.1, key="pbr_range", label_visibility="collapsed") if use_pbr else None

use_roe = st.sidebar.checkbox("ROE下限 (%)", value=True)
roe_min = st.sidebar.slider("ROE下限", 0, 30, 8, key="roe_min", label_visibility="collapsed") if use_roe else None

use_div = st.sidebar.checkbox("配当利回り下限 (%)", value=True)
div_yield_min = st.sidebar.slider("配当利回り下限", 0.0, 10.0, 2.0, 0.1, key="div_yield", label_visibility="collapsed") if use_div else None

use_rev = st.sidebar.checkbox("売上成長率下限 (%)", value=True)
rev_growth_min = st.sidebar.slider("売上成長率下限", -20, 50, 5, key="rev_growth", label_visibility="collapsed") if use_rev else None

use_profit = st.sidebar.checkbox("利益成長率下限 (%)", value=True)
profit_growth_min = st.sidebar.slider("利益成長率下限", -50, 200, 5, key="profit_growth", label_visibility="collapsed") if use_profit else None

use_rsi = st.sidebar.checkbox("RSI範囲", value=True)
rsi_range = st.sidebar.slider("RSI範囲", 0, 100, (40, 70), key="rsi_range", label_visibility="collapsed") if use_rsi else None

above_ma25 = st.sidebar.checkbox("25日MA上のみ", value=True)

use_volume = st.sidebar.checkbox("平均出来高下限", value=True)
volume_avg_min = st.sidebar.number_input("平均出来高下限", min_value=0, value=100000, step=10000, key="vol_min", label_visibility="collapsed") if use_volume else None

# ── 追加フィルター ──
volume_surge  = st.sidebar.checkbox("出来高急増（平均の2倍以上）", value=False)
high_roe      = st.sidebar.checkbox("高ROE優先（ROE 15%以上）", value=False)
near_52w_high = st.sidebar.checkbox("52週高値圏（直近高値の90%以上）", value=False)

# ── モメンタムフィルター ──
use_mom = st.sidebar.checkbox("🎯 モメンタムシグナルあり", value=False)
if use_mom:
    mom_days_max = st.sidebar.slider("シグナル直近N日以内", 7, 60, 30)
    mom_vol_min  = st.sidebar.slider("出来高倍率下限", 2.0, 5.0, 3.0, 0.5)
    mom_combo    = st.sidebar.checkbox("上方修正×モメンタムのみ", value=False)
else:
    mom_days_max = 30
    mom_vol_min  = 2.0
    mom_combo    = False
pullback_pct = st.sidebar.slider("押し目目標 (%)", 1.0, 5.0, 2.0, 0.5)

# =========================================================
# データ更新処理（統合）
# =========================================================
if update_button:
    with st.status(
        "全銘柄データを取得中... (初回は財務データ取得のため数十分かかります。2回目以降は数分で完了します)",
        expanded=True,
    ) as status:
        try:
            progress_bar = st.progress(0)
            log_area     = st.empty()

            def progress_callback(i, total, msg):
                progress_bar.progress((i + 1) / total)
                if i % 50 == 0:
                    log_area.write(f"処理中... {i+1}/{total}  {msg}")

            cache_df = fetch_all_stocks(market_codes=None, progress_callback=progress_callback)
            progress_bar.progress(1.0)
            cache_updated = get_cache_updated_at()
            status.update(label=f"✅ データ更新完了 ({len(cache_df)} 銘柄)", state="complete")
        except Exception as e:
            st.error(f"データ更新中にエラーが発生しました: {e}")
            status.update(label="エラーが発生しました", state="error")

# =========================================================
# Section 1: ファンダ×テクニカル スクリーニング
# =========================================================
st.subheader("📊 ファンダ×テクニカル スクリーニング")

if cache_df is not None and not cache_df.empty:
    df = cache_df.copy()

    if market_codes and "market" in df.columns:
        market_name_map = {
            "0111": ["東証プライム", "プライム"],
            "0112": ["東証スタンダード", "スタンダード"],
            "0113": ["東証グロース", "グロース"],
        }
        target_names = []
        for c in market_codes:
            target_names.extend(market_name_map.get(c, [c]))
        df = df[df["market"].isin(target_names)]

    if use_per and per_max is not None and "PER" in df.columns:
        df = df[df["PER"].notna() & (df["PER"] > 0) & (df["PER"] <= per_max)]
    if use_pbr and pbr_range is not None and "PBR" in df.columns:
        df = df[df["PBR"].notna() & (df["PBR"] >= pbr_range[0]) & (df["PBR"] <= pbr_range[1])]
    if use_roe and roe_min is not None and "ROE" in df.columns:
        df = df[df["ROE"].notna() & (df["ROE"] >= roe_min)]
    if use_div and div_yield_min is not None and "div_yield" in df.columns:
        df = df[df["div_yield"].notna() & (df["div_yield"] >= div_yield_min)]
    if use_rev and rev_growth_min is not None and "rev_growth" in df.columns:
        df = df[df["rev_growth"].notna() & (df["rev_growth"] >= rev_growth_min)]
    if use_profit and profit_growth_min is not None and "profit_growth" in df.columns:
        df = df[df["profit_growth"].notna() & (df["profit_growth"] >= profit_growth_min)]
    if use_rsi and rsi_range is not None and "RSI" in df.columns:
        df = df[df["RSI"].notna() & (df["RSI"] >= rsi_range[0]) & (df["RSI"] <= rsi_range[1])]
    if above_ma25 and "close" in df.columns and "MA25" in df.columns:
        df = df[df["close"] >= df["MA25"]]
    if use_volume and volume_avg_min is not None and "avg_volume" in df.columns:
        df = df[df["avg_volume"] >= volume_avg_min]

    # 追加フィルター
    if volume_surge and "latest_volume" in df.columns and "avg_volume" in df.columns:
        df = df[df["latest_volume"] > df["avg_volume"] * 2]
    if high_roe and "ROE" in df.columns:
        df = df[df["ROE"] > 15]

    # 52週高値フィルター（prices.parquetから計算）
    if near_52w_high and not df.empty and os.path.exists(PRICES_PATH):
        prices_52w = pd.read_parquet(PRICES_PATH)
        prices_52w["Date"] = pd.to_datetime(prices_52w["Date"])
        cutoff_52w = pd.Timestamp.today() - pd.Timedelta(days=365)
        keep = []
        for _, row in df.iterrows():
            code5 = str(row.get("code", ""))
            cp = prices_52w[
                (prices_52w["Code"] == code5) &
                (prices_52w["Date"] >= cutoff_52w)
            ]
            if cp.empty:
                keep.append(False)
                continue
            high_52w = pd.to_numeric(cp["AdjH"], errors="coerce").max()
            current  = float(row.get("close", 0))
            keep.append(current >= high_52w * 0.9)
        df = df[keep]

    # モメンタムフィルター
    if use_mom and "mom_signal" in df.columns:
        df = df[df["mom_signal"] == True]
        if "mom_signal_date" in df.columns:
            df = df.copy()
            df["_sig_dt"] = pd.to_datetime(df["mom_signal_date"], format="%Y/%m/%d", errors="coerce")
            df = df[df["_sig_dt"] >= pd.Timestamp.today() - pd.Timedelta(days=mom_days_max)]
            df = df.drop(columns=["_sig_dt"])
        if mom_vol_min > 2.0 and "mom_vol_ratio" in df.columns:
            df = df[df["mom_vol_ratio"] >= mom_vol_min]
        if mom_combo and "mom_revision" in df.columns:
            df = df[df["mom_revision"].notna()]

    result_df = df.sort_values("score", ascending=False).head(display_top_n_funda).reset_index(drop=True)

    # 押し目目標列を追加
    if "mom_signal_close" in result_df.columns:
        result_df["押し目目標(円)"] = (
            result_df["mom_signal_close"] * (1 - pullback_pct / 100)
        ).round(0)

    ts_str  = cache_updated.strftime('%Y/%m/%d %H:%M') if cache_updated else "不明"
    n_mom   = int(result_df["mom_signal"].sum())   if "mom_signal"   in result_df.columns else 0
    n_combo = int(result_df["mom_revision"].notna().sum()) if "mom_revision" in result_df.columns else 0
    caption = f"📦 {ts_str} のデータ ｜ {len(df)} 件マッチ（上位 {len(result_df)} 件表示）"
    if n_mom > 0:
        caption += f"　🎯 モメンタム: {n_mom} 件　🔥 上方修正×モメンタム: {n_combo} 件"
    st.caption(caption)

    if result_df.empty:
        st.warning("条件に合致する銘柄が見つかりませんでした。フィルターを緩めてください。")
    else:
        cols_order = [
            "code_4", "company_name", "market", "close", "score",
            "PER", "PBR", "ROE", "div_yield", "rev_growth", "profit_growth", "RSI",
            "mom_signal", "mom_vol_ratio", "mom_gc", "mom_new_high", "mom_macd",
            "mom_ma200_ratio", "mom_revision", "mom_signal_date",
            "mom_signal_close", "押し目目標(円)",
        ]
        display_df = result_df[[c for c in cols_order if c in result_df.columns]].copy()
        if "mom_revision" in display_df.columns:
            display_df["mom_revision"] = display_df["mom_revision"].apply(
                lambda v: f"🔥+{v:.1f}%" if pd.notna(v) else ""
            )
        col_config = {
            "code_4"          : st.column_config.TextColumn("銘柄コード"),
            "company_name"    : st.column_config.TextColumn("会社名"),
            "market"          : st.column_config.TextColumn("市場"),
            "close"           : st.column_config.NumberColumn("株価(円)", format="%.0f"),
            "score"           : st.column_config.ProgressColumn("ファンダスコア", min_value=0, max_value=90, format="%.1f"),
            "PER"             : st.column_config.NumberColumn("PER", format="%.2f"),
            "PBR"             : st.column_config.NumberColumn("PBR", format="%.2f"),
            "ROE"             : st.column_config.NumberColumn("ROE(%)", format="%.2f"),
            "div_yield"       : st.column_config.NumberColumn("配当利回り(%)", format="%.2f"),
            "rev_growth"      : st.column_config.NumberColumn("売上成長(%)", format="%.1f"),
            "profit_growth"   : st.column_config.NumberColumn("利益成長(%)", format="%.1f"),
            "RSI"             : st.column_config.NumberColumn("RSI", format="%.1f"),
            "mom_signal"      : st.column_config.CheckboxColumn("モメンタム"),
            "mom_vol_ratio"   : st.column_config.NumberColumn("出来高倍率", format="%.1f倍"),
            "mom_gc"          : st.column_config.CheckboxColumn("GC"),
            "mom_new_high"    : st.column_config.CheckboxColumn("52W高値"),
            "mom_macd"        : st.column_config.CheckboxColumn("MACD"),
            "mom_ma200_ratio" : st.column_config.NumberColumn("MA200比(%)", format="+%.1f%%"),
            "mom_revision"    : st.column_config.TextColumn("上方修正"),
            "mom_signal_date" : st.column_config.TextColumn("シグナル日"),
            "mom_signal_close": st.column_config.NumberColumn("シグナル日終値(円)", format="%.0f"),
            "押し目目標(円)"   : st.column_config.NumberColumn(f"押し目{pullback_pct:.1f}%目標(円)", format="%.0f"),
        }
        selection = st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            column_config=col_config,
            on_select="rerun",
            selection_mode="single-row",
        )
        if selection and selection.selection and selection.selection.rows:
            idx           = selection.selection.rows[0]
            selected_code = result_df.iloc[idx]["code"]
            code4         = selected_code[:4]
            name          = result_df.iloc[idx].get("company_name", "")
            col_a, col_b, col_c = st.columns([2, 1, 1])
            with col_a:
                st.success(f"選択中: {code4} {name}")
            with col_b:
                if st.button("📈 詳細を見る", type="primary", key="detail"):
                    st.session_state["selected_code"] = selected_code
                    st.switch_page("pages/2_stock_detail.py")
            with col_c:
                if st.button("★ ウォッチ追加", key="watch"):
                    sig_close = result_df.iloc[idx].get("mom_signal_close")
                    try:
                        target = int(float(sig_close) * (1 - pullback_pct / 100)) if sig_close and not np.isnan(float(sig_close)) else None
                    except Exception:
                        target = None
                    if _add_to_watchlist(code4, name, target):
                        st.success(f"✅ {name} をウォッチリストに追加しました")
                        st.rerun()
                    else:
                        st.info("すでに登録済みです")

        st.download_button(
            "📥 CSVダウンロード",
            data=result_df.to_csv(index=False, encoding="utf-8-sig"),
            file_name=f"screening_result_{datetime.today().strftime('%Y%m%d')}.csv",
            mime="text/csv",
        )
else:
    st.info("データが未取得です。サイドバーの **「🔄 データ更新」** でデータを取得してください（初回・1日1回）。")

# =========================================================
# Section 3: ウォッチリスト
# =========================================================
st.markdown("---")
st.subheader("📌 ウォッチリスト")

wl_items = _load_watchlist()
if not wl_items:
    st.caption("ウォッチリストはまだ空です。スクリーニング結果から銘柄を選択して「★ ウォッチ追加」ボタンで追加できます。")
else:
    _cache     = load_cache()
    _price_map = {}
    if _cache is not None and "close" in _cache.columns and "code" in _cache.columns:
        for _, row in _cache.iterrows():
            c4 = str(row["code"])[:4]
            _price_map[c4] = row["close"]

    wl_df  = pd.DataFrame(wl_items)
    edited = st.data_editor(
        wl_df[["code", "name", "target_price", "memo", "added_at"]].assign(
            現在値=lambda df: df["code"].map(lambda c: _price_map.get(c)),
            乖離率=lambda df: df.apply(
                lambda r: (
                    lambda pct: f"🎯 目標達成 ({pct:+.1f}%)" if pct <= 0 else f"あと▲{pct:.1f}%"
                )((r["現在値"] - r["target_price"]) / r["target_price"] * 100)
                if r["target_price"] and r["現在値"] else "─",
                axis=1,
            ),
        ).rename(columns={
            "code": "コード", "name": "会社名", "target_price": "目標(円)",
            "memo": "メモ",   "added_at": "追加日",
        }),
        column_config={
            "コード"   : st.column_config.TextColumn("コード", width="small"),
            "会社名"   : st.column_config.TextColumn("会社名"),
            "目標(円)" : st.column_config.NumberColumn("目標(円)", format="%d"),
            "メモ"     : st.column_config.TextColumn("メモ（直接編集可）", width="large"),
            "追加日"   : st.column_config.TextColumn("追加日", width="small"),
            "現在値"   : st.column_config.NumberColumn("現在値(円)", format="%d"),
            "乖離率"   : st.column_config.TextColumn("目標まで"),
        },
        disabled=["コード", "会社名", "目標(円)", "追加日", "現在値", "乖離率"],
        use_container_width=True,
        hide_index=True,
        key="watchlist_editor",
    )
    if edited is not None:
        for i, row in edited.iterrows():
            wl_items[i]["memo"] = row["メモ"]
        _save_watchlist(wl_items)

    del_names = [f"{i['code']} {i['name']}" for i in wl_items]
    col_del1, col_del2 = st.columns([3, 1])
    with col_del1:
        del_sel = st.selectbox("削除する銘柄", ["─ 選択 ─"] + del_names, key="wl_del_sel")
    with col_del2:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("🗑 削除", key="wl_del_btn") and del_sel != "─ 選択 ─":
            del_code = del_sel.split(" ")[0]
            wl_items = [i for i in wl_items if i["code"] != del_code]
            _save_watchlist(wl_items)
            st.rerun()

    jump_names = [f"{i['code']} {i['name']}" for i in wl_items]
    col_j1, col_j2 = st.columns([3, 1])
    with col_j1:
        jump_sel = st.selectbox("詳細を見る", ["─ 選択 ─"] + jump_names, key="wl_jump_sel")
    with col_j2:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("📈 詳細", key="wl_jump_btn") and jump_sel != "─ 選択 ─":
            jump_code = jump_sel.split(" ")[0] + "0"
            st.session_state["selected_code"] = jump_code
            st.switch_page("pages/2_stock_detail.py")

    trade_names = [f"{i['code']} {i['name']}" for i in wl_items]
    col_t1, col_t2 = st.columns([3, 1])
    with col_t1:
        trade_sel = st.selectbox("トレード記録する", ["─ 選択 ─"] + trade_names, key="wl_trade_sel")
    with col_t2:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("📓 記録", key="wl_trade_btn") and trade_sel != "─ 選択 ─":
            trade_code = trade_sel.split(" ")[0]
            trade_name = " ".join(trade_sel.split(" ")[1:])
            matched = next((i for i in wl_items if i["code"] == trade_code), {})
            st.session_state["prefill_ticker"] = trade_code
            st.session_state["prefill_name"]   = trade_name
            st.session_state["prefill_target"] = matched.get("target_price", 0)
            st.switch_page("pages/6_trade_log.py")
