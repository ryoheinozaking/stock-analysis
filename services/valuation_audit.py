# -*- coding: utf-8 -*-
"""
バリュエーション指標の答え合わせ（自前計算 vs J-Quants 公式の算出値）

自前: stock_cache の PER / PBR / ROE と、パイプラインの時価総額（close × 本決算の ShOutFY）
JPX : data/valuation.parquet（/v2/equities/valuation の FwdPER / PBR / FwdROE / MktCap）

【あらかじめ分かっている定義の差】（一致しなくてもバグではない「正しいずれ」）
- 株数: 自前は発行済株式数（自己株を含む）、JPX は自己株を除いた株数
  → 自前の PBR は「発行済 ÷（発行済 − 自己株）」倍だけ大きく出る
- 時価総額: 上記に加え、自前は最新の本決算時点の株数、JPX は直近の株数（期中の消却・増資でずれる）
- 予想ROE: 自前の分母は最新の本決算の自己資本、JPX は直近四半期末の自己資本
- 反映日: JPX は決算短信を開示の翌営業日から反映する
これらと株式分割の調整ずれで説明できない差を「要調査」とし、自前計算のバグ候補として扱う。
"""
from typing import Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

from services.fins_utils import disclosure_freshness, filter_fy_statements, filter_statements

TOLERANCE = 0.05   # 自前 / JPX の比が ±5% 以内なら一致とみなす

CAT_MATCH      = "一致"
CAT_TREASURY   = "自己株の定義差"
CAT_EQ_TIMING  = "自己資本の時点差"
CAT_SPLIT      = "分割の調整ずれ"
CAT_JPX_LAG    = "JPX未反映（評価日以降の開示）"
CAT_OURS_STALE = "自前の決算データが古い"
CAT_OURS_NULL  = "自前が空欄"
CAT_JPX_NULL   = "JPXが空欄"
CAT_UNKNOWN    = "要調査"
ALL_CATEGORIES = [CAT_MATCH, CAT_TREASURY, CAT_EQ_TIMING, CAT_SPLIT, CAT_JPX_LAG,
                  CAT_OURS_STALE, CAT_OURS_NULL, CAT_JPX_NULL, CAT_UNKNOWN]

# ours: 自前の列 / jpx: valuation の列 / jpx_scale: 自前の単位に揃える倍率
# explain: 定義差で説明できる場合の (分類名, 期待される「自前/JPX」の比の列)
METRIC_SPECS = [
    {"metric": "PER（予想）", "ours": "PER", "jpx": "FwdPER", "jpx_scale": 1.0,
     "explain": []},
    {"metric": "PBR", "ours": "PBR", "jpx": "PBR", "jpx_scale": 1.0,
     "explain": [(CAT_TREASURY, "treasury_ratio")]},
    {"metric": "ROE（予想）", "ours": "ROE", "jpx": "FwdROE", "jpx_scale": 100.0,   # 自前は%、JPX は小数
     "explain": [(CAT_EQ_TIMING, "eq_ratio")]},
    {"metric": "時価総額", "ours": "market_cap", "jpx": "MktCap", "jpx_scale": 1e6,  # 自前は円、JPX は百万円
     "explain": [(CAT_TREASURY, "mcap_share_ratio")]},
]

_REF_COLS = ["treasury_ratio", "mcap_share_ratio", "eq_ratio", "last_disc_date", "fins_overdue"]


def classify(ours, jpx, explains: Iterable[Tuple[str, float]], split_factor,
             disclosed_on_or_after: bool, fins_overdue: bool,
             tol: float = TOLERANCE) -> Tuple[Optional[str], float]:
    """1銘柄・1指標のずれを分類し (分類名, 自前/JPX の比) を返す。両方空欄なら分類名は None。"""
    if pd.isna(ours) and pd.isna(jpx):
        return None, np.nan
    if pd.isna(ours):
        return CAT_OURS_NULL, np.nan
    if pd.isna(jpx):
        return CAT_JPX_NULL, np.nan
    ratio = ours / jpx if jpx != 0 else np.nan
    if pd.isna(ratio) or ratio <= 0:
        return CAT_UNKNOWN, ratio          # 符号が逆（赤字の扱いの違いなど）
    if abs(ratio - 1) <= tol:
        return CAT_MATCH, ratio
    for label, expected in explains:
        if pd.notna(expected) and expected > 0 and abs(ratio / expected - 1) <= tol:
            return label, ratio
    if pd.notna(split_factor) and split_factor > 0 and abs(split_factor - 1) > tol:
        if any(abs(ratio / s - 1) <= tol for s in (split_factor, 1.0 / split_factor)):
            return CAT_SPLIT, ratio
    if disclosed_on_or_after:
        return CAT_JPX_LAG, ratio
    if fins_overdue:
        return CAT_OURS_STALE, ratio
    return CAT_UNKNOWN, ratio


def build_reference_ratios(fins_df: pd.DataFrame, as_of) -> pd.DataFrame:
    """定義差で説明できるかを判定するための「期待される比」と開示の鮮度を銘柄別に返す。

    列: code / treasury_ratio / mcap_share_ratio / eq_ratio / last_disc_date / fins_overdue
    - treasury_ratio   = 発行済 ÷（発行済 − 自己株）          … 直近の決算短信
    - mcap_share_ratio = 本決算の発行済 ÷（直近の発行済 − 直近の自己株）
    - eq_ratio         = 直近の自己資本 ÷ 本決算の自己資本
    """
    stmts = filter_statements(fins_df).copy()
    for col in ("ShOutFY", "TrShFY", "Eq"):
        stmts[col] = pd.to_numeric(stmts[col], errors="coerce")
    stmts["_disc"] = pd.to_datetime(stmts["DiscDate"], errors="coerce")
    stmts = stmts.sort_values("_disc")
    fy_idx = set(filter_fy_statements(stmts).index)

    rows = []
    for code, g in stmts.groupby("Code", sort=False):
        latest_rows = g.dropna(subset=["ShOutFY", "Eq"])
        fy_rows = g[g.index.isin(fy_idx)]
        latest = latest_rows.iloc[-1] if not latest_rows.empty else None
        fy = fy_rows.iloc[-1] if not fy_rows.empty else None

        ex_treasury = np.nan
        treasury_ratio = np.nan
        if latest is not None and pd.notna(latest["TrShFY"]):
            ex_treasury = latest["ShOutFY"] - latest["TrShFY"]
            if ex_treasury > 0:
                treasury_ratio = latest["ShOutFY"] / ex_treasury
        mcap_share_ratio = (fy["ShOutFY"] / ex_treasury
                            if fy is not None and pd.notna(fy["ShOutFY"]) and ex_treasury > 0
                            else np.nan)
        eq_ratio = (latest["Eq"] / fy["Eq"]
                    if latest is not None and fy is not None and pd.notna(fy["Eq"]) and fy["Eq"] > 0
                    else np.nan)
        rows.append({"code": code, "treasury_ratio": treasury_ratio,
                     "mcap_share_ratio": mcap_share_ratio, "eq_ratio": eq_ratio})

    refs = pd.DataFrame(rows, columns=["code", "treasury_ratio", "mcap_share_ratio", "eq_ratio"])
    fresh = disclosure_freshness(fins_df, as_of)[["code", "last_disc_date", "fins_overdue"]]
    return refs.merge(fresh, on="code", how="outer")


def recent_split_factors(prices_df: pd.DataFrame, as_of, days: int = 365) -> pd.DataFrame:
    """評価日までの直近 days 日の AdjFactor 累積積（分割がなければ 1.0）を銘柄別に返す。"""
    as_of = pd.Timestamp(as_of)
    dates = pd.to_datetime(prices_df["Date"], errors="coerce")
    mask = (dates > as_of - pd.Timedelta(days=days)) & (dates <= as_of)
    adj = pd.to_numeric(prices_df.loc[mask, "AdjFactor"], errors="coerce").fillna(1.0)
    sf = adj.groupby(prices_df.loc[mask, "Code"]).prod()
    return pd.DataFrame({"code": sf.index, "split_factor": sf.values})


def audit_valuation(ours: pd.DataFrame, jpx: pd.DataFrame, refs: pd.DataFrame,
                    splits: pd.DataFrame, tol: float = TOLERANCE) -> pd.DataFrame:
    """自前と JPX を銘柄×指標で突き合わせ、ずれを分類した縦持ち DataFrame を返す。

    ours: code / PER / PBR / ROE(%) / market_cap(円) / company_name（任意）
    jpx:  valuation.parquet の 1 日分（Date / Code / FwdPER / PBR / FwdROE / MktCap ...）
    戻り値の列: code / company_name / metric / ours / jpx / ratio / category / last_disc_date / split_factor
    """
    out_cols = ["code", "company_name", "metric", "ours", "jpx", "ratio", "category",
                "last_disc_date", "split_factor"]
    if jpx.empty or ours.empty:
        return pd.DataFrame(columns=out_cols)

    jpx_date = pd.to_datetime(jpx["Date"]).max().normalize()
    j = jpx.rename(columns={c: f"jpx_{c}" for c in jpx.columns if c != "Code"}).rename(columns={"Code": "code"})
    df = (ours.merge(j, on="code", how="inner")
              .merge(refs, on="code", how="left")
              .merge(splits, on="code", how="left")
              .reset_index(drop=True))
    for col in _REF_COLS + ["split_factor"]:
        if col not in df.columns:
            df[col] = np.nan
    if "company_name" not in df.columns:
        df["company_name"] = ""

    disclosed_after = (pd.to_datetime(df["last_disc_date"], errors="coerce") >= jpx_date).to_numpy()
    overdue = df["fins_overdue"].map(lambda v: bool(v) if pd.notna(v) else False).to_numpy()
    split_factor = pd.to_numeric(df["split_factor"], errors="coerce").to_numpy()

    parts: List[pd.DataFrame] = []
    for spec in METRIC_SPECS:
        jpx_col = f"jpx_{spec['jpx']}"
        if spec["ours"] not in df.columns or jpx_col not in df.columns:
            continue
        ours_v = pd.to_numeric(df[spec["ours"]], errors="coerce").to_numpy()
        jpx_v = pd.to_numeric(df[jpx_col], errors="coerce").to_numpy() * spec["jpx_scale"]
        explain_vals = [(label, pd.to_numeric(df[col], errors="coerce").to_numpy())
                        for label, col in spec["explain"]]

        cats, ratios = [], []
        for i in range(len(df)):
            cat, ratio = classify(ours_v[i], jpx_v[i],
                                  [(label, vals[i]) for label, vals in explain_vals],
                                  split_factor[i], disclosed_after[i], overdue[i], tol)
            cats.append(cat)
            ratios.append(ratio)

        part = pd.DataFrame({
            "code": df["code"], "company_name": df["company_name"], "metric": spec["metric"],
            "ours": ours_v, "jpx": jpx_v, "ratio": ratios, "category": cats,
            "last_disc_date": df["last_disc_date"], "split_factor": split_factor,
        })
        parts.append(part[part["category"].notna()])

    if not parts:
        return pd.DataFrame(columns=out_cols)
    return pd.concat(parts, ignore_index=True)[out_cols]


def summarize(audit: pd.DataFrame) -> pd.DataFrame:
    """指標ごとに、比較銘柄数・一致率・自前/JPX の中央値・分類別の件数を返す。"""
    rows = []
    for metric, g in audit.groupby("metric", sort=False):
        counts = g["category"].value_counts()
        ratios = pd.to_numeric(g["ratio"], errors="coerce").dropna()
        row = {
            "metric": metric,
            "比較銘柄数": len(g),
            "一致": int(counts.get(CAT_MATCH, 0)),
            "一致率(%)": round(counts.get(CAT_MATCH, 0) / len(g) * 100, 1),
            "自前/JPX中央値": round(float(ratios.median()), 3) if not ratios.empty else np.nan,
        }
        for cat in ALL_CATEGORIES[1:]:
            row[cat] = int(counts.get(cat, 0))
        rows.append(row)
    return pd.DataFrame(rows)
