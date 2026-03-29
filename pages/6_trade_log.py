# pages/6_trade_log.py
# -*- coding: utf-8 -*-
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))

import streamlit as st
import pandas as pd
from datetime import date

from services.trade_log_service import load, add_entry, add_exit

st.set_page_config(page_title="トレードログ", page_icon="📓", layout="wide")
st.title("📓 トレードログ")
st.caption("実トレードの記録・集計・自己分析")

tab1, tab2, tab3, tab4 = st.tabs(["📝 新規エントリー", "📊 保有中", "📋 履歴", "📈 統計"])

with tab1:
    st.subheader("新規エントリー記録")
    prefill_ticker = st.session_state.pop("prefill_ticker", "")
    prefill_name   = st.session_state.pop("prefill_name", "")

    with st.form("entry_form"):
        col1, col2 = st.columns(2)
        with col1:
            ticker       = st.text_input("銘柄コード（4桁）", value=prefill_ticker, placeholder="例: 7203")
            if prefill_name:
                st.caption(f"📌 {prefill_name} からの引き継ぎ")
            date_entry   = st.date_input("エントリー日", value=date.today())
            entry_price  = st.number_input("エントリー価格（円）", min_value=0.0, step=1.0)
            stop_price   = st.number_input("損切りライン（円）", min_value=0.0, step=1.0)
        with col2:
            position_pct  = st.number_input("ポジションサイズ（%）", min_value=0.0, max_value=100.0, step=0.5, value=5.0)
            strategy_type = st.selectbox("戦略タイプ", ["momentum", "earnings", "growth", "theme"])
            memo          = st.text_area("メモ", height=100)

        submitted = st.form_submit_button("✅ エントリーを記録", use_container_width=True)

    if submitted:
        if not ticker or entry_price <= 0:
            st.error("銘柄コードとエントリー価格は必須です")
        else:
            with st.spinner("データを自動取得中..."):
                try:
                    add_entry(
                        ticker=ticker.strip(),
                        date_entry=str(date_entry),
                        entry_price=entry_price,
                        stop_price=stop_price,
                        position_pct=position_pct,
                        strategy_type=strategy_type,
                        memo=memo,
                    )
                    st.success(f"✅ {ticker} のエントリーを記録しました（RSI・出来高比率・財務指標を自動取得）")
                    st.rerun()
                except Exception as e:
                    st.error(f"エラー: {e}")

with tab2:
    st.subheader("保有中ポジション")
    df = load()
    open_pos = df[df["date_exit"].isna() | (df["date_exit"] == "")].copy()

    if open_pos.empty:
        st.info("保有中のポジションはありません")
    else:
        disp_cols = ["id", "ticker", "company_name", "date_entry", "entry_price", "stop_price", "position_pct", "strategy_type", "rsi_at_entry", "memo"]
        st.dataframe(open_pos[[c for c in disp_cols if c in open_pos.columns]], use_container_width=True, hide_index=True)

        st.divider()
        st.subheader("エグジット記録")
        with st.form("exit_form"):
            open_ids = open_pos["id"].tolist()
            labels   = [f"ID {r['id']}: {r['ticker']} {r.get('company_name','')} （{r['date_entry']} @{r['entry_price']}円）"
                        for _, r in open_pos.iterrows()]
            selected_label = st.selectbox("決済するポジション", labels)
            selected_id    = open_ids[labels.index(selected_label)] if labels else None

            col1, col2 = st.columns(2)
            with col1:
                date_exit  = st.date_input("決済日", value=date.today())
                exit_price = st.number_input("決済価格（円）", min_value=0.0, step=1.0)
            with col2:
                rule_violation = st.checkbox("ルール違反トレード")

            exit_submitted = st.form_submit_button("✅ 決済を記録", use_container_width=True)

        if exit_submitted and selected_id and exit_price > 0:
            with st.spinner("MFE/MAE を計算中..."):
                try:
                    add_exit(
                        trade_id=str(selected_id),
                        date_exit=str(date_exit),
                        exit_price=exit_price,
                        rule_violation=rule_violation,
                    )
                    st.success("✅ 決済を記録しました")
                    st.rerun()
                except Exception as e:
                    st.error(f"エラー: {e}")

with tab3:
    st.subheader("トレード履歴")
    df = load()
    closed = df[df["date_exit"].notna() & (df["date_exit"] != "")].copy()

    if closed.empty:
        st.info("まだ決済済みトレードはありません")
    else:
        col1, col2 = st.columns(2)
        with col1:
            strategy_filter = st.multiselect("戦略タイプ", ["momentum", "earnings", "growth", "theme"],
                                              default=["momentum", "earnings", "growth", "theme"])
        with col2:
            show_violations = st.checkbox("ルール違反のみ表示", value=False)

        if strategy_filter:
            closed = closed[closed["strategy_type"].isin(strategy_filter)]
        if show_violations:
            closed = closed[closed["rule_violation"] == True]

        def _color_pnl(val):
            try:
                v = float(val)
                if v > 0:
                    return "color: #2ecc71"
                elif v < 0:
                    return "color: #e74c3c"
            except Exception:
                pass
            return ""

        disp = ["id", "ticker", "company_name", "date_entry", "date_exit",
                "entry_price", "exit_price", "pnl_pct", "holding_days",
                "max_profit_pct", "max_loss_pct", "strategy_type", "rule_violation", "memo"]
        disp = [c for c in disp if c in closed.columns]

        st.dataframe(
            closed[disp].style.map(_color_pnl, subset=["pnl_pct"] if "pnl_pct" in disp else []),
            use_container_width=True,
            hide_index=True,
        )

with tab4:
    st.subheader("統計・分析")
    df = load()
    closed = df[df["date_exit"].notna() & (df["date_exit"] != "") & df["pnl_pct"].notna()].copy()
    closed["pnl_pct"] = pd.to_numeric(closed["pnl_pct"], errors="coerce")
    closed["win"]     = closed["pnl_pct"] > 0

    if len(closed) < 3:
        st.info("統計には最低3件の決済済みトレードが必要です")
    else:
        total    = len(closed)
        wins     = closed["win"].sum()
        win_rate = wins / total * 100
        avg_pnl  = closed["pnl_pct"].mean()
        avg_win  = closed[closed["win"]]["pnl_pct"].mean()
        avg_loss = closed[~closed["win"]]["pnl_pct"].mean()

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("総トレード数", total)
        c2.metric("勝率", f"{win_rate:.1f}%")
        c3.metric("平均リターン", f"{avg_pnl:+.2f}%")
        c4.metric("プロフィットファクター",
                  f"{abs(avg_win/avg_loss):.2f}" if avg_loss != 0 and not pd.isna(avg_loss) else "∞")

        st.divider()

        st.subheader("戦略別成績")
        if "strategy_type" in closed.columns:
            strategy_stats = closed.groupby("strategy_type").agg(
                件数=("pnl_pct", "count"),
                勝率=("win", lambda x: f"{x.mean()*100:.1f}%"),
                平均リターン=("pnl_pct", lambda x: f"{x.mean():+.2f}%"),
                中央値=("pnl_pct", lambda x: f"{x.median():+.2f}%"),
            ).reset_index()
            st.dataframe(strategy_stats, use_container_width=True, hide_index=True)

        st.divider()

        st.subheader("ルール遵守 vs 違反")
        if "rule_violation" in closed.columns:
            rule_stats = closed.groupby("rule_violation").agg(
                件数=("pnl_pct", "count"),
                勝率=("win", lambda x: f"{x.mean()*100:.1f}%"),
                平均リターン=("pnl_pct", lambda x: f"{x.mean():+.2f}%"),
            ).reset_index()
            rule_stats["rule_violation"] = rule_stats["rule_violation"].map({True: "違反あり", False: "遵守"})
            st.dataframe(rule_stats, use_container_width=True, hide_index=True)

        st.divider()

        st.subheader("エントリー時 RSI 別成績")
        if "rsi_at_entry" in closed.columns:
            closed["rsi_at_entry"] = pd.to_numeric(closed["rsi_at_entry"], errors="coerce")
            rsi_data = closed.dropna(subset=["rsi_at_entry"]).copy()
            if not rsi_data.empty:
                bins   = [0, 30, 40, 50, 60, 70, 100]
                labels = ["〜30", "30〜40", "40〜50", "50〜60", "60〜70", "70〜"]
                rsi_data["rsi_band"] = pd.cut(rsi_data["rsi_at_entry"], bins=bins, labels=labels)
                rsi_stats = rsi_data.groupby("rsi_band", observed=True).agg(
                    件数=("pnl_pct", "count"),
                    勝率=("win", lambda x: f"{x.mean()*100:.1f}%"),
                    平均リターン=("pnl_pct", lambda x: f"{x.mean():+.2f}%"),
                ).reset_index()
                st.dataframe(rsi_stats, use_container_width=True, hide_index=True)
