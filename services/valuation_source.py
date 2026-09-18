# -*- coding: utf-8 -*-
"""
J-Quants バリュエーション指標（/v2/equities/valuation）で PER / PBR / ROE / 時価総額を置き換える

本番パイプライン（stock_cache）とバックテスト（過去時点スナップショット）で共通に使う。

【2026-09-18 導入の経緯】
自前計算は答え合わせ（scripts/audit_valuation.py）で4種類のバグが見つかった:
  分割予定会社の予想EPS（分割後ベース開示）/ 予想修正レコードが最新だと予想EPSを見失う /
  PBR・ROE が純資産ベース（非支配株主持分を含む）/ 時価総額が本決算時点の株数
JPX 値に置き換えたバックテスト（2022-06〜2025-12・43 月次起点・Top20・250日）は
α・勝率とも自前計算と同等（差は t≦1.0）で、予想ベースの PER は予測力（IC）が最も強かった。
→ 本番は予想ベース（FwdPER・FwdROE）を使う。
"""
import pandas as pd

JPX_MAX_STALENESS_DAYS = 7    # as_of からこの日数より古い値は使わない（月末が休日でも直前営業日は拾える）
LIVE_PER_BASIS = "forward"    # 本番パイプラインの PER / ROE の基準（会社予想ベース）


def apply_jpx_valuation(
    snap:      pd.DataFrame,
    valuation: pd.DataFrame,
    as_of,
    per_basis: str = "ttm",
) -> pd.DataFrame:
    """code 列を持つ DataFrame の PER / PBR / ROE / 時価総額を J-Quants の値で置き換える。

    - PER: per_basis="ttm" は直近12ヶ月実績ベースの PER、"forward" は会社予想ベースの FwdPER
    - ROE: 同じく ROE / FwdROE（J-Quants は小数なので 100 倍して % にそろえる）
    - PBR: 自己資本・自己株控除後の株数ベース
    - market_cap: 自己株控除後の株数 × 株価（百万円 → 円）。apply_hard_filter がこの列を使う
    - valuation_date: 使った値の日付（YYYY-MM-DD）
    J-Quants 側が空欄の銘柄は自前値で埋めない（定義を混ぜないため）。
    決算短信は開示の翌営業日から反映されるので、as_of 当日の開示は含まれない。
    """
    if per_basis not in ("ttm", "forward"):
        raise ValueError(f"per_basis は ttm か forward: {per_basis}")
    as_of = pd.Timestamp(as_of)
    dates = (valuation["Date"] if pd.api.types.is_datetime64_any_dtype(valuation["Date"])
             else pd.to_datetime(valuation["Date"], errors="coerce"))
    mask = (dates <= as_of) & (dates > as_of - pd.Timedelta(days=JPX_MAX_STALENESS_DAYS))
    latest = (valuation[mask].assign(_date=dates[mask])
              .sort_values("_date").groupby("Code").tail(1).set_index("Code"))

    def _num(col):
        return out["code"].map(pd.to_numeric(latest[col], errors="coerce"))

    out = snap.copy()
    forward = per_basis == "forward"
    out["PER"] = _num("FwdPER" if forward else "PER")
    out["PBR"] = _num("PBR")
    roe_col = "FwdROE" if forward else "ROE"
    if roe_col in latest.columns:
        out["ROE"] = _num(roe_col) * 100
    out["market_cap"] = _num("MktCap") * 1e6
    out["valuation_date"] = out["code"].map(latest["_date"].dt.strftime("%Y-%m-%d"))
    return out


def apply_live_valuation(stock_df: pd.DataFrame, valuation: pd.DataFrame, as_of) -> pd.DataFrame:
    """本番の stock_cache 用。自前計算の PER / PBR / ROE は *_self 列に残す（答え合わせ用）。"""
    out = stock_df.copy()
    for col in ("PER", "PBR", "ROE"):
        if col in out.columns:
            out[f"{col}_self"] = out[col]
    return apply_jpx_valuation(out, valuation, as_of, per_basis=LIVE_PER_BASIS)
