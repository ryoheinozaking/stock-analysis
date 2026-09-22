# -*- coding: utf-8 -*-
"""
ペーパー運用（バリュー株 Top10・単元未満株・20 位より下で売る）

設計書: docs/superpowers/specs/2026-09-22-value-paper-trading-design.md
状態は data/paper/value_top10.json（ローカルのみ。Streamlit Cloud には無い）。
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from services.split_adjust import normalize_close
from services.value_paper import CAPITAL, PARAMS, load_state, run_update

st.set_page_config(page_title="ペーパー運用", layout="wide")
st.title("ペーパー運用（バリュー株 Top10）")
st.caption(
    f"仮想資金 {CAPITAL / 1e4:,.0f} 万円。毎月末の順位で上位 {PARAMS.n_hold} 銘柄を均等に単元未満株（1 株単位）で保有し、"
    f"{PARAMS.exit_rank} 位より下に落ちた・ハードフィルタで外れた銘柄を翌営業日の寄付きで売る。"
    "約定は翌営業日の寄付き、片道 0.1% のずれを差し引く。「スクリーニング」の「データ更新」の後に自動で処理される"
)

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

if st.button("ペーパー運用を更新", help="前回処理した日の翌営業日から、株価データの最終日までを処理する"):
    with st.spinner("処理中（月末をまたぐと順位の計算に 1 分ほどかかります）..."):
        try:
            r = run_update()
            st.success(f"{len(r['processed'])} 日分を処理しました（処理済み: {r['last_processed']} / "
                       f"株価データの最終日: {r['latest_data']}）")
        except Exception as e:
            st.error(f"更新に失敗しました: {e}")

broker = load_state()
if broker is None:
    st.info("まだ始めていません。上のボタンを押すと、株価データの最終日の順位で買い注文を作り、運用を始めます。")
    st.stop()


@st.cache_data(show_spinner=False)
def _prices_for(codes: tuple, since: str, mtime: float) -> pd.DataFrame:
    p = pd.read_parquet(os.path.join(_ROOT, "data", "prices.parquet"), columns=["Date", "Code", "C", "AdjFactor"])
    p["Date"] = pd.to_datetime(p["Date"])
    return p[p["Code"].astype(str).isin(codes) & (p["Date"] >= pd.Timestamp(since) - pd.Timedelta(days=400))]


@st.cache_data(show_spinner=False)
def _names(mtime: float) -> pd.Series:
    s = pd.read_parquet(os.path.join(_ROOT, "data", "stock_cache.parquet"), columns=["code", "company_name"])
    return s.set_index(s["code"].astype(str))["company_name"]


_mtime = os.path.getmtime(os.path.join(_ROOT, "data", "prices.parquet"))
names = _names(os.path.getmtime(os.path.join(_ROOT, "data", "stock_cache.parquet")))
curve = pd.DataFrame(broker.equity_curve)
start = curve["date"].iloc[0] if len(curve) else broker.last_processed
latest_data = pd.read_parquet(os.path.join(_ROOT, "data", "prices.parquet"), columns=["Date"])["Date"].max()

# ── 状況 ────────────────────────────────────────────────────────────────
tp = _prices_for(("13060",), start, _mtime).sort_values("Date").reset_index(drop=True)
topix = pd.Series(normalize_close(tp, dropna=False).values, index=tp["Date"])
topix = topix[topix.index >= pd.Timestamp(start)]
eq = broker.last_equity()
c1, c2, c3, c4 = st.columns(4)
c1.metric("資産", f"{eq:,.0f} 円", f"{(eq / CAPITAL - 1) * 100:+.2f}%")
if len(curve) and len(topix):
    t_ret = topix.reindex(pd.to_datetime(curve["date"])).ffill().iloc[-1] / topix.iloc[0] - 1
    c2.metric("TOPIX（同じ期間）", f"{t_ret * 100:+.2f}%")
c3.metric("保有 / 注文待ち", f"{len(broker.positions)} 銘柄 / {len(broker.orders)} 件")
c4.metric("処理済みの日", broker.last_processed, f"株価データ {pd.Timestamp(latest_data):%Y-%m-%d}", delta_color="off")
st.caption(f"開始日 {start}")

if pd.Timestamp(broker.last_processed) < pd.Timestamp(latest_data):
    st.warning("株価データの最終日まで処理されていません。上のボタンで更新してください"
               "（最新日が月末かどうか未確定の日は、翌月のデータが入るまで処理を保留します）。")

# ── 資産の推移 ──────────────────────────────────────────────────────────
if len(curve) > 1:
    x = pd.to_datetime(curve["date"])
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=x, y=curve["equity"], name="ペーパー口座", line=dict(color="#1f3a5f", width=2)))
    if len(topix):
        tb = topix.reindex(x).ffill()
        fig.add_trace(go.Scatter(x=x, y=CAPITAL * tb / tb.iloc[0], name="TOPIX を同額で保有",
                                 line=dict(color="#9aa5b1", width=1.5, dash="dot")))
    fig.update_layout(height=320, margin=dict(l=10, r=10, t=10, b=10), yaxis_title="円",
                      legend=dict(orientation="h", y=1.08))
    st.plotly_chart(fig, use_container_width=True)

# ── 保有 ────────────────────────────────────────────────────────────────
st.subheader("保有")
if broker.positions:
    rows = []
    for code, p in broker.positions.items():
        value = p["shares"] * p["last_close"]
        rows.append({"コード": code[:4], "銘柄": names.get(code, ""), "株数": p["shares"],
                     "買付日": p["entry_date"], "買値": round(p["entry_price"], 1), "終値": p["last_close"],
                     "評価額": round(value), "損益率(%)": round((p["last_close"] / p["entry_price"] - 1) * 100, 2),
                     "比率(%)": round(value / eq * 100, 1)})
    st.dataframe(pd.DataFrame(rows).sort_values("評価額", ascending=False), hide_index=True,
                 use_container_width=True)
    st.caption(f"現金 {broker.cash:,.0f} 円")
else:
    st.write("保有はありません。")

# ── 注文待ち ────────────────────────────────────────────────────────────
st.subheader("次の営業日の寄付きで約定する注文")
if broker.orders:
    o = pd.DataFrame(broker.orders)
    o["銘柄"] = o["code"].map(names)
    o["コード"] = o["code"].str[:4]
    o["売買"] = o["side"].map({"buy": "買い", "sell": "売り"})
    st.dataframe(o[["売買", "コード", "銘柄", "rank", "signal_date"]].rename(
        columns={"rank": "判断時の順位", "signal_date": "判断した日"}), hide_index=True, use_container_width=True)
    st.caption(f"買いは 1 銘柄あたり 資産 ÷ {PARAMS.n_hold}（約 {eq / PARAMS.n_hold:,.0f} 円）を上限に、1 株単位で買う")
else:
    st.write("注文はありません（次の見直しは月末の最終営業日）。")

# ── 取引の履歴 ──────────────────────────────────────────────────────────
st.subheader("決済済みの取引")
if broker.trades:
    t = pd.DataFrame(broker.trades)
    t["銘柄"] = t["code"].map(names)
    t["コード"] = t["code"].str[:4]
    t["理由"] = t["reason"].map({"rank": "順位低下", "delisted": "上場廃止"}).fillna(t["reason"])
    t["損益率(%)"] = (t["pnl_pct"] * 100).round(2)
    t["損益"] = t["pnl"].round()
    st.dataframe(t[["コード", "銘柄", "entry_date", "exit_date", "shares", "entry_price", "exit_price",
                    "損益", "損益率(%)", "理由"]].rename(columns={
                        "entry_date": "買付日", "exit_date": "売却日", "shares": "株数",
                        "entry_price": "買値", "exit_price": "売値"}).iloc[::-1],
                 hide_index=True, use_container_width=True)
    wins = (t["pnl"] > 0).mean() * 100
    st.caption(f"{len(t)} 件 / 勝率 {wins:.0f}% / 確定損益 {t['pnl'].sum():,.0f} 円")
else:
    st.write("まだありません。")
