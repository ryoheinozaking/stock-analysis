# -*- coding: utf-8 -*-
"""
ブレイクアウト順張りの指標と売買判定（副作用なし）

設計書: docs/superpowers/specs/2026-09-22-breakout-paper-trading-design.md

株価スケール:
- 指標（移動平均・55 日高値・出来高倍率・ATR）は split_adjust で末尾日スケールに正規化した系列で計算
- 売買は「その日の生の株価」で行うため、ATR は atr_raw = ATR(正規化) ÷ cum_factor でその日のスケールに戻す
"""
from dataclasses import dataclass
from typing import Iterable, Optional, Set, Tuple

import numpy as np
import pandas as pd

from services.split_adjust import (
    cum_factor, normalize_close, normalize_high, normalize_low, normalize_volume,
)


@dataclass(frozen=True)
class BreakoutParams:
    breakout_days: int = 55          # 前日までの N 日高値を終値で上抜け
    ma_fast: int = 50
    ma_slow: int = 200
    ma_slow_slope_days: int = 20     # 200 日線が N 日前より上 = 上向き
    vol_avg_days: int = 50
    vol_mult: float = 1.5            # 当日出来高 ≥ 前日までの 50 日平均 × 1.5
    atr_days: int = 20
    init_stop_atr: float = 2.0       # 初期損切り = 約定値 − ATR × 2（株数の計算にも使う）
    trail_atr: float = 3.0           # トレーリング = 保有後の最高値 − ATR × 3
    risk_pct: float = 0.005          # 1 銘柄の最大損失 = 資産の 0.5%
    max_pos_pct: float = 0.20        # 1 銘柄の金額上限 = 資産の 20%
    lot: int = 100
    min_mktcap_mn: float = 50_000    # 時価総額 500 億円（valuation.parquet は百万円単位）
    min_turnover: float = 5e8        # 20 日平均売買代金 5 億円
    turnover_days: int = 20
    earnings_buffer_days: int = 3    # 決算発表予定日まで 3 営業日以内なら買わない
    slippage: float = 0.001          # 片道 0.1%
    market_code: str = "13060"       # TOPIX 連動 ETF
    max_hold_days: Optional[int] = None   # 保有の上限（営業日）。到達日の大引けで売る
    pullback_lookback: int = 5       # 押し目: 直近 5 日の安値が
    pullback_touch: float = 1.02     #         50 日線 × 1.02 以下まで下げた


def compute_indicators(cp: pd.DataFrame, p: BreakoutParams) -> pd.DataFrame:
    """1 銘柄分の日足から指標と、テクニカル条件（上昇トレンド + 出来高を伴う高値更新）を計算する。

    cp: prices.parquet の 1 銘柄分（Date, Code, O, H, L, C, Vo, Va, AdjFactor）。
    売買停止などで C が欠けた行は除く。
    """
    cp = cp[pd.to_numeric(cp["C"], errors="coerce").notna()].sort_values("Date").reset_index(drop=True)
    cum = cum_factor(cp)
    nc = normalize_close(cp, dropna=False)
    nh = normalize_high(cp, dropna=False)
    nl = normalize_low(cp, dropna=False)
    nv = normalize_volume(cp)

    ma_fast = nc.rolling(p.ma_fast).mean()
    ma_slow = nc.rolling(p.ma_slow).mean()
    ma_slow_prev = ma_slow.shift(p.ma_slow_slope_days)
    high_prev = nh.shift(1).rolling(p.breakout_days).max()
    vol_avg = nv.shift(1).rolling(p.vol_avg_days).mean()
    vol_ratio = nv / vol_avg

    prev_c = nc.shift(1)
    tr = pd.concat([nh - nl, (nh - prev_c).abs(), (nl - prev_c).abs()], axis=1).max(axis=1)
    atr_norm = tr.rolling(p.atr_days).mean()

    uptrend = (nc > ma_slow) & (ma_fast > ma_slow) & (ma_slow > ma_slow_prev)
    tech = uptrend & (nc > ma_fast) & (nc > high_prev) & (vol_ratio >= p.vol_mult)
    # 押し目買い: 上昇トレンド中に 50 日線付近まで下げ、終値が前日高値と 50 日線を上回って反発
    touched = nl.rolling(p.pullback_lookback).min() <= ma_fast * p.pullback_touch
    pullback = uptrend & touched & (nc > nh.shift(1)) & (nc > ma_fast)

    return pd.DataFrame({
        "Date": pd.to_datetime(cp["Date"]),
        "Code": cp["Code"].astype(str),
        "O": pd.to_numeric(cp["O"], errors="coerce"),
        "H": pd.to_numeric(cp["H"], errors="coerce"),
        "L": pd.to_numeric(cp["L"], errors="coerce"),
        "C": pd.to_numeric(cp["C"], errors="coerce"),
        "AdjFactor": pd.to_numeric(cp["AdjFactor"], errors="coerce").fillna(1.0),
        "atr_raw": atr_norm / cum,
        "vol_ratio": vol_ratio,
        "turnover_avg": pd.to_numeric(cp["Va"], errors="coerce").rolling(p.turnover_days).mean(),
        "tech_signal": tech.fillna(False).astype(bool),
        "pullback_signal": pullback.fillna(False).astype(bool),
    })


def market_filter(cp_market: pd.DataFrame, p: BreakoutParams) -> pd.Series:
    """TOPIX 連動 ETF の終値が 200 日線より上の日 = True（Date を index にした Series）。"""
    cp = cp_market[pd.to_numeric(cp_market["C"], errors="coerce").notna()].sort_values("Date")
    cp = cp.reset_index(drop=True)
    nc = normalize_close(cp, dropna=False)
    ok = (nc > nc.rolling(p.ma_slow).mean()).fillna(False).astype(bool)
    ok.index = pd.to_datetime(cp["Date"])
    return ok


def earnings_block_set(earnings: pd.DataFrame, trading_dates: Iterable,
                       buffer_days: int) -> Set[Tuple[str, pd.Timestamp]]:
    """決算発表予定日まで buffer_days 営業日以内の (銘柄, 日) の集合。

    予定の公表・変更は新しい行として追加されるので、(銘柄, 決算区分, 決算期末) ごとに
    「その日までに公表された最後の予定」を使う（先読みしない）。予定日が空欄（未定）は使わない。
    止めるのは予定日の buffer_days 営業日前〜前営業日。
    """
    td = pd.DatetimeIndex(sorted(pd.to_datetime(list(trading_dates))))
    e = pd.DataFrame({
        "code": earnings["Code"].astype(str),
        "fq": earnings["FQName"].astype(str),
        "fye": earnings["FYE"].astype(str),
        "pub": pd.to_datetime(earnings["PubDate"], errors="coerce"),
        "sch": pd.to_datetime(earnings["SchDate"], errors="coerce"),
    }).dropna(subset=["pub"]).sort_values("pub")
    e["next_pub"] = e.groupby(["code", "fq", "fye"])["pub"].shift(-1)
    e = e.dropna(subset=["sch"])

    out: Set[Tuple[str, pd.Timestamp]] = set()
    s_idx = td.searchsorted(e["sch"].values, side="left")
    lo_idx = np.maximum(s_idx - buffer_days, 0)
    pub_idx = td.searchsorted(e["pub"].values, side="left")
    nxt = e["next_pub"].values
    nxt_idx = np.where(pd.isna(nxt), len(td), td.searchsorted(nxt, side="left"))
    for code, lo, hi_s, p_i, n_i in zip(e["code"].values, lo_idx, s_idx, pub_idx, nxt_idx):
        start, end = max(lo, p_i), min(hi_s, n_i)
        for i in range(start, end):
            out.add((code, td[i]))
    return out


def revision_events(fins: pd.DataFrame, threshold: float = 0.20) -> pd.DataFrame:
    """同じ年度の会社予想の純利益（FNP）を threshold 以上引き上げた開示 (Code, DiscDate)。

    純利益は株数に依存しないので分割でずれない。本決算の短信は CurFYEn が終わった年度なので比較に使わない。
    前回の予想が 0 以下（赤字予想）からの変化は率が意味を持たないので対象外。
    """
    f = fins[["Code", "DiscDate", "DocType", "CurFYEn", "FNP"]].copy()
    f = f[~f["DocType"].astype(str).str.startswith("FYFinancialStatements")]
    f["FNP"] = pd.to_numeric(f["FNP"], errors="coerce")
    f["DiscDate"] = pd.to_datetime(f["DiscDate"], errors="coerce")
    f = f.dropna(subset=["FNP", "DiscDate", "CurFYEn"])
    f["Code"] = f["Code"].astype(str)
    f = f.sort_values("DiscDate").drop_duplicates(["Code", "CurFYEn", "DiscDate"], keep="last")
    f["prev"] = f.groupby(["Code", "CurFYEn"])["FNP"].shift(1)
    ev = f[(f["prev"] > 0) & (f["FNP"] / f["prev"] - 1 >= threshold)]
    return ev[["Code", "DiscDate"]].reset_index(drop=True)


def revision_window_set(events: pd.DataFrame, trading_dates: Iterable,
                        window_days: int = 60) -> Set[Tuple[str, pd.Timestamp]]:
    """上方修正の開示日から window_days 暦日以内の (銘柄, 営業日) の集合。"""
    td = pd.DatetimeIndex(sorted(pd.to_datetime(list(trading_dates))))
    out: Set[Tuple[str, pd.Timestamp]] = set()
    for code, d in zip(events["Code"].astype(str), pd.to_datetime(events["DiscDate"])):
        lo = td.searchsorted(d, side="left")
        hi = td.searchsorted(d + pd.Timedelta(days=window_days), side="right")
        for i in range(lo, hi):
            out.add((code, td[i]))
    return out


def pbr_lower_half_set(universe: pd.DataFrame) -> Set[Tuple[str, pd.Timestamp]]:
    """その日の対象銘柄（Date, Code, PBR）の中で PBR が中央値以下の (銘柄, 日)。PBR が無い・0 以下は除く。"""
    u = universe.copy()
    u["PBR"] = pd.to_numeric(u["PBR"], errors="coerce")
    u = u[u["PBR"] > 0]
    med = u.groupby("Date")["PBR"].transform("median")
    u = u[u["PBR"] <= med]
    return set(zip(u["Code"].astype(str), pd.to_datetime(u["Date"])))


def build_candidates(ind: pd.DataFrame, mktcap: pd.DataFrame, company_codes: Iterable[str],
                     market_ok: pd.Series, earn_block: Set[Tuple[str, pd.Timestamp]],
                     p: BreakoutParams, signal_col: str = "tech_signal",
                     require: Optional[Set[Tuple[str, pd.Timestamp]]] = None) -> pd.DataFrame:
    """買い条件をすべて満たした (Date, Code, vol_ratio, atr_raw, C) を返す。

    ind: compute_indicators を全銘柄分つなげたもの
    mktcap: (Date, Code, MktCap[百万円])。その日の値が無い銘柄は時価総額条件を満たさない扱い
    company_codes: 対象にする銘柄（バックテスト = 決算短信のある会社、ペーパー運用 = プライム）
    signal_col: "tech_signal"（ブレイクアウト）/ "pullback_signal"（押し目買い）
    require: 指定すると、この (銘柄, 日) の集合に入る候補だけ残す（割安・上方修正の条件）
    """
    c = ind[ind[signal_col]].copy()
    c = c[c["Code"].isin(set(company_codes)) & (c["turnover_avg"] >= p.min_turnover)]
    mc = mktcap[["Date", "Code", "MktCap"]].copy()
    mc["Date"] = pd.to_datetime(mc["Date"])
    mc["Code"] = mc["Code"].astype(str)
    c = c.merge(mc, on=["Date", "Code"], how="left")
    c = c[c["MktCap"] >= p.min_mktcap_mn]
    c = c[c["Date"].map(market_ok).fillna(False).astype(bool)]
    if earn_block:
        keep = [(cd, d) not in earn_block for cd, d in zip(c["Code"], c["Date"])]
        c = c[np.array(keep, dtype=bool)]
    if require is not None:
        keep = [(cd, d) in require for cd, d in zip(c["Code"], c["Date"])]
        c = c[np.array(keep, dtype=bool)]
    c = c[c["atr_raw"] > 0]
    return (c[["Date", "Code", "vol_ratio", "atr_raw", "C"]]
            .sort_values(["Date", "vol_ratio"], ascending=[True, False]).reset_index(drop=True))
