# -*- coding: utf-8 -*-
"""
株式分割調整ユーティリティ

【背景】
J-Quants の AdjC（権利修正済終値）は API を叩いた時点でのスナップショット値。
日次インクリメンタル取得 + 分割イベント発生 という組み合わせでは、
prices.parquet 内の AdjC が時期によって異なるスケールになる（混在状態）。

例: サンリオ 81360（2024-03-28 1:3 分割、2026-03-30 1:5 分割）
    2021-04 〜 2024-03-27: AdjC/C = 1/15 （両方反映）
    2024-04 〜 2025-11    : AdjC/C = 1/5  （2026分割のみ反映）
    2025-12 〜 2026-03-29 : AdjC/C = 1.0  （未調整）
    2026-03-30 以降       : AdjC/C = 1.0  （生値が分割後）

【方針】
AdjC は使わず、生の C と AdjFactor から都度正規化する（業界標準）。
- normalize_close(cp): 各日の C を「最新日スケール」に正規化した系列
- normalize_volume(cp): 同じく出来高を最新日スケールに正規化
- split_factor_between(cp, from, to): from < d <= to の AdjFactor 累積積
   per-share 値（EPS, BPS, DPS）を from_date スケールから to_date スケールに変換するときに使う
   per-share 値: × split_factor （1:5分割なら ×0.2 で値が小さくなる）
   shares 数  : / split_factor （1:5分割なら ×5 で株数が増える）
"""
import pandas as pd
import numpy as np


def cum_factor(cp: pd.DataFrame) -> pd.Series:
    """
    各行 i に対して prod(AdjFactor[j] for j > i) を返す。
    つまり「その行より後（未来側）に発生する分割の累積積」。

    最新日（last row）の値は 1.0（未来の分割なし）。
    分割発生日 d においては cum_factor[d] は d より後の分割の累積積。

    生 OHLC × cum_factor で「末尾日スケール」に統一できる。
    """
    af = pd.to_numeric(cp["AdjFactor"], errors="coerce").fillna(1.0)
    rev_cumprod = af.iloc[::-1].cumprod().iloc[::-1]
    cum = rev_cumprod.shift(-1).fillna(1.0)
    return cum


# 後方互換: _cum_factor （アンダースコア版）も残しておく
_cum_factor = cum_factor


def _normalize_price(cp: pd.DataFrame, col: str, dropna: bool) -> pd.Series:
    """OHLC 各列を末尾日スケールに正規化する内部実装。"""
    raw = pd.to_numeric(cp[col], errors="coerce")
    cum = cum_factor(cp)
    out = raw * cum
    return out.dropna() if dropna else out


def normalize_close(cp: pd.DataFrame, dropna: bool = True) -> pd.Series:
    """
    cp の C 系列を「cp 末尾のスケール」に正規化して返す。

    cp は単一銘柄の prices DataFrame。Date 順に並んでいる前提。
    インデックスは cp と一致する（dropna=False のとき）。

    引数:
      dropna: True なら NaN 行を除外（デフォルト）。MA/RSI 等の単独計算用。
              False なら NaN を保持して cp とインデックスを揃える。

    用途: MA / RSI / MACD など長期にわたる close 系列指標の計算。
    """
    return _normalize_price(cp, "C", dropna)


def normalize_open(cp: pd.DataFrame, dropna: bool = True) -> pd.Series:
    """O 系列を末尾日スケールに正規化（OHLC chart 用）。"""
    return _normalize_price(cp, "O", dropna)


def normalize_high(cp: pd.DataFrame, dropna: bool = True) -> pd.Series:
    """H 系列を末尾日スケールに正規化（高値ブレイク・52週高値判定用）。"""
    return _normalize_price(cp, "H", dropna)


def normalize_low(cp: pd.DataFrame, dropna: bool = True) -> pd.Series:
    """L 系列を末尾日スケールに正規化（52週安値判定用）。"""
    return _normalize_price(cp, "L", dropna)


def normalize_volume(cp: pd.DataFrame, fillna: bool = True) -> pd.Series:
    """
    cp の Vo 系列を「cp 末尾のスケール（株数ベース）」に正規化して返す。

    分割で 1株 → N株 になると出来高（株数）も N 倍になるため、
    per-share の close と整合させるには Vo / cum_factor が正規化値。
    例: 1:5 分割（cum_factor=0.2）の場合、過去の出来高を /0.2=×5 して
        分割後株数ベースに揃える。

    引数:
      fillna: True なら NaN/欠損行を 0 で埋める（デフォルト）。
              False なら NaN を保持して cp とインデックスを揃える。
    """
    raw_Vo = pd.to_numeric(cp["Vo"], errors="coerce")
    cum    = cum_factor(cp)
    # cum が 0 になることはないが念のためゼロ除算回避
    out = raw_Vo / cum.replace(0, np.nan)
    return out.fillna(0) if fillna else out


def split_factor_between(
    cp: pd.DataFrame,
    from_date,
    to_date,
) -> float:
    """
    from_date < day <= to_date 区間の AdjFactor 累積積を返す。

    per-share の値 V を from_date スケールから to_date スケールに変換するとき:
        V_at_to_date = V_at_from_date × split_factor_between(cp, from_date, to_date)

    例: 1:5 分割（AdjFactor=0.2）が区間内にある場合、戻り値は 0.2。
        EPS（pre-split=100円）× 0.2 = 20円（post-split per-share 値）

    shares 数を変換する場合は 戻り値の逆数を掛ける（または値を割る）:
        shares_at_to_date = shares_at_from_date / split_factor_between(...)

    引数:
      cp: 単一銘柄の prices DataFrame（Date 列必須）
      from_date: 起点日（この日 自身は含まない）
      to_date: 終点日（この日 自身を含む）

    分割イベントが区間内に存在しなければ 1.0 を返す。
    """
    if from_date is None or to_date is None:
        return 1.0
    fd = pd.to_datetime(from_date, errors="coerce")
    td = pd.to_datetime(to_date,   errors="coerce")
    if pd.isna(fd) or pd.isna(td) or fd >= td:
        return 1.0
    cp_dates = pd.to_datetime(cp["Date"], errors="coerce")
    af       = pd.to_numeric(cp["AdjFactor"], errors="coerce").fillna(1.0)
    mask     = (cp_dates > fd) & (cp_dates <= td)
    if not mask.any():
        return 1.0
    return float(af[mask].prod())


def forecast_per_share_multiplier(
    per_share,
    aggregate,
    shares,
    split_factor: float,
    *,
    max_rel_err: float = 0.25,
) -> float:
    """予想 per-share 値（FEPS / FDivAnn 等）を「開示スケール → 最新日スケール」に
    変換するための乗数を返す。

    【背景】分割発表後・効力前に開示された通期予想は、会社により
    分割前ベース・分割後ベースのどちらでも開示され得る（例: 6227 ＡＩメカテックは
    2026-03-30 効力の 1:3 分割を、2026-02-13 の 2Q 短信で「分割後ベース」の
    予想 EPS=163.93 として開示）。「開示日 < 分割日 → 分割前ベース」という日付
    ルールは後者を誤判定し、split_factor を二重に掛けて PER を 3 倍過大にする。

    そこで予想純利益 ``aggregate``（FNP）÷ 期末発行済株式数 ``shares``（ShOutFY）と
    突合してスケールを判定する:
      - ``per_share`` ≈ aggregate/shares              → 分割前ベース → 乗数 = split_factor
      - ``per_share`` ≈ aggregate/shares × split_factor → 分割後ベース → 乗数 = 1.0

    判定不能な場合（split_factor==1.0 / per_share・aggregate・shares 欠損 /
    両候補から max_rel_err 超乖離）は従来の日付ルール（分割前前提）に従い
    ``split_factor`` を返す。1 レコード内の予想 per-share 値（EPS と DPS）は
    同一ベースで開示されるため、EPS から得た乗数を DPS にも流用してよい。
    """
    sf = float(split_factor) if split_factor is not None and not pd.isna(split_factor) else 1.0
    # 分割なし or 予想値が無い → 判定不要（呼び出し側は per_share 欠損時この戻り値を使わない）
    if sf == 1.0 or per_share is None or pd.isna(per_share) or float(per_share) == 0.0:
        return sf
    # アンカー（FNP/shares）が無ければ従来動作にフォールバック
    if aggregate is None or pd.isna(aggregate) or shares is None or pd.isna(shares) or float(shares) == 0.0:
        return sf
    cand_pre = float(aggregate) / float(shares)   # 分割前 EPS 候補
    if cand_pre == 0.0:
        return sf
    cand_post = cand_pre * sf                       # 分割後 EPS 候補
    ps = float(per_share)
    d_pre = abs(ps / cand_pre - 1.0)
    d_post = abs(ps / cand_post - 1.0) if cand_post != 0.0 else float("inf")
    # どちらの候補からも大きく外れる（連単不一致・株数が古い等）→ 信頼できずフォールバック
    if min(d_pre, d_post) > max_rel_err:
        return sf
    return 1.0 if d_post < d_pre else sf


def adjust_per_share(value: float, split_factor: float) -> float:
    """per-share 値（EPS, BPS, DPS）に分割係数を適用してスケール変換。"""
    if value is None or pd.isna(value) or pd.isna(split_factor):
        return value
    return float(value) * float(split_factor)


def adjust_shares(value: float, split_factor: float) -> float:
    """shares 数を分割係数で割ってスケール変換（分割で株数増加）。"""
    if value is None or pd.isna(value) or pd.isna(split_factor) or split_factor == 0:
        return value
    return float(value) / float(split_factor)
