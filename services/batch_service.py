# -*- coding: utf-8 -*-
"""
全銘柄メトリクス一括取得バッチ（バルク取得・差分更新版）

処理フロー:
  1. update_prices()     : /equities/bars/daily?date= で日付ループ → prices.parquet に差分追記
  2. update_fins()       : 初回=全銘柄ループ、以降=当日開示分のみ → fins_cache.parquet に差分追記
  3. build_stock_cache() : parquet読み込み→スコア計算→stock_cache.csv（APIコールなし）
  4. fetch_all_stocks()  : 上記3ステップをまとめた後方互換ラッパー
"""

import os
import time
import requests
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

from screener import calc_rsi, calc_moving_average, calc_avg_volume, calc_signal_score

_ROOT       = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE_PATH  = os.path.join(_ROOT, "data", "stock_cache.parquet")
PRICES_PATH = os.path.join(_ROOT, "data", "prices.parquet")
FINS_PATH   = os.path.join(_ROOT, "data", "fins_cache.parquet")
BASE_URL    = "https://api.jquants.com/v2"


# ─── 共通ユーティリティ ──────────────────────────────────────────

def _api_key():
    key = os.getenv("JQUANTS_API_KEY")
    if not key:
        raise EnvironmentError(".env に JQUANTS_API_KEY が設定されていません")
    return key


def _get(endpoint, params=None, retry=3):
    headers = {"x-api-key": _api_key()}
    for attempt in range(retry):
        r = requests.get(
            f"{BASE_URL}{endpoint}",
            params=params or {},
            headers=headers,
            timeout=30,
        )
        if r.status_code == 429:
            time.sleep(60)
            continue
        r.raise_for_status()
        return r.json()
    r.raise_for_status()


# ─── 既存インターフェース（変更なし） ────────────────────────────

def load_cache():
    """stock_cache.parquet を読み込む。なければ・空ならば None を返す。"""
    if os.path.exists(CACHE_PATH):
        try:
            df = pd.read_parquet(CACHE_PATH)
            # code列を文字列に統一
            for col in ("code", "code_4"):
                if col in df.columns:
                    df[col] = df[col].astype(str)
            return df if not df.empty else None
        except Exception:
            return None
    return None


def get_cache_updated_at():
    """キャッシュの最終更新日時を返す。なければ None。"""
    if os.path.exists(CACHE_PATH):
        return datetime.fromtimestamp(os.path.getmtime(CACHE_PATH))
    return None


# ─── 価格データ（バルク・差分更新） ─────────────────────────────

def _load_prices():
    if os.path.exists(PRICES_PATH):
        return pd.read_parquet(PRICES_PATH)
    return pd.DataFrame()


def update_prices(progress_callback=None):
    """
    差分更新: 前回保存日の翌日〜今日分をバルクAPIで取得して追記する。

    1日1回のAPIコールで全銘柄のOHLCVが取得できるため、
    「銘柄ループ×日数」から「日付ループ」に変わり大幅に削減される。
    """
    existing = _load_prices()

    if not existing.empty:
        last_date = pd.to_datetime(existing["Date"]).max()
        from_dt   = last_date + timedelta(days=1)
    else:
        from_dt = datetime.today() - timedelta(days=400)  # 52週高値検出に必要

    to_dt = datetime.today()

    if from_dt.date() > to_dt.date():
        return existing  # 既に最新

    # 平日リスト（土日除外・祝日はAPIが空レスポンスを返すのでスキップ）
    date_strs = [
        d.strftime("%Y-%m-%d")
        for d in pd.date_range(from_dt, to_dt, freq="B")
    ]

    new_frames = []
    total = len(date_strs)
    for i, date_str in enumerate(date_strs):
        if progress_callback:
            progress_callback(i, total, f"📅 価格取得: {date_str}")
        try:
            data = _get("/equities/bars/daily", {"date": date_str})
            df   = pd.DataFrame(data.get("data", []))
            if not df.empty:
                new_frames.append(df)
        except Exception:
            pass  # 祝日など取得できない日はスキップ

    if not new_frames:
        return existing

    new_data = pd.concat(new_frames, ignore_index=True)
    result   = pd.concat([existing, new_data], ignore_index=True) if not existing.empty else new_data

    # 同日・同コードの重複除去
    result = result.drop_duplicates(subset=["Date", "Code"]).reset_index(drop=True)

    os.makedirs(os.path.dirname(PRICES_PATH), exist_ok=True)
    result.to_parquet(PRICES_PATH, index=False)
    return result


# ─── 財務データ（初回全件・以降差分） ───────────────────────────

def _load_fins():
    if os.path.exists(FINS_PATH):
        return pd.read_parquet(FINS_PATH)
    return pd.DataFrame()


def update_fins(progress_callback=None):
    """
    財務データ更新。

    - fins_cache.parquet が存在しない → 全銘柄を1件ずつ取得（初回のみ）
    - 存在する               → 当日開示分だけ差分更新（1回のAPIコール）
    """
    existing = _load_fins()

    if existing.empty:
        return _fetch_fins_all(progress_callback)

    # 差分更新: 今日の開示分を取得
    today = datetime.today().strftime("%Y-%m-%d")
    try:
        data   = _get("/fins/summary", {"date": today})
        new_df = pd.DataFrame(data.get("data", []))
    except Exception:
        return existing

    if new_df.empty:
        return existing

    # 追記して重複除去（DiscNo がユニークキー）
    result = pd.concat([existing, new_df], ignore_index=True)
    if "DiscNo" in result.columns:
        result = result.drop_duplicates(subset=["DiscNo"]).reset_index(drop=True)

    result.to_parquet(FINS_PATH, index=False)
    return result


def _fetch_fins_all(progress_callback=None):
    """
    初回のみ実行: 全銘柄の財務データを1件ずつ取得して保存する。
    取得成功後は fins_cache.parquet が存在するため、次回以降は差分更新になる。
    """
    data      = _get("/equities/master")
    codes     = [row["Code"] for row in data.get("data", []) if row.get("Code")]
    all_fins  = []
    total     = len(codes)

    for i, code in enumerate(codes):
        if progress_callback:
            progress_callback(i, total, f"📊 財務取得: {code}")
        try:
            d  = _get("/fins/summary", {"code": code})
            df = pd.DataFrame(d.get("data", []))
            if not df.empty:
                # 全種別（FY/1Q/2Q/3Q）・全件保存（上方修正検出のため履歴が必要）
                all_fins.append(df)
        except Exception:
            pass

    result = pd.concat(all_fins, ignore_index=True) if all_fins else pd.DataFrame()
    os.makedirs(os.path.dirname(FINS_PATH), exist_ok=True)
    result.to_parquet(FINS_PATH, index=False)
    return result


# ─── モメンタムシグナル計算（APIコールなし） ────────────────────

def _empty_mom():
    return {
        "mom_signal"      : False,
        "mom_signal_date" : "",
        "mom_signal_close": np.nan,
        "mom_vol_ratio"   : np.nan,
        "mom_gc"          : False,
        "mom_new_high"    : False,
        "mom_macd"        : False,
        "mom_above_ma200" : False,
        "mom_ma200_ratio" : np.nan,
        "mom_revision"    : np.nan,
        # SEPA
        "sepa_stage"      : 0,
        "sepa_ma_align"   : False,
        "sepa_ma200_trend": False,
        "sepa_from_low"   : np.nan,
        "sepa_from_high"  : np.nan,
        "sepa_rs"         : np.nan,
        # グランビル・ダウ理論
        "gran_g1"    : False,
        "gran_g2"    : False,
        "dow_uptrend": False,
    }


def _get_revision(fins_df, code, threshold_pct=20, window_days=30):
    try:
        # fins_df はコード別グループ化済みを前提（フィルター不要）
        cf = fins_df.copy()
        if cf.empty:
            return np.nan
        # 通期（FY）のみ対象（四半期間比較による誤検知を防ぐ）
        if "CurPerType" in cf.columns:
            cf = cf[cf["CurPerType"] == "FY"]
        cf["DiscDate"] = pd.to_datetime(cf["DiscDate"], errors="coerce")
        cf["FEPS"]     = pd.to_numeric(cf["FEPS"], errors="coerce")
        cf = (cf.dropna(subset=["FEPS", "DiscDate"])
                .query("FEPS > 0")
                .sort_values("DiscDate")
                .reset_index(drop=True))
        cutoff = pd.Timestamp.today() - pd.Timedelta(days=window_days)
        for i in range(1, len(cf)):
            if cf.loc[i, "DiscDate"] < cutoff:
                continue
            prev_eps = cf.loc[i - 1, "FEPS"]
            if prev_eps == 0:
                continue
            rev = (cf.loc[i, "FEPS"] - prev_eps) / abs(prev_eps) * 100
            if rev >= threshold_pct:
                return round(rev, 1)
        return np.nan
    except Exception:
        return np.nan


def _calc_sepa(close: pd.Series, topix_close: pd.Series) -> dict:
    """
    ミネルヴィニSEPA条件を評価してステージ分類と各指標を返す。

    Stage 2（買いゾーン）の条件:
      1. MA50 > MA150 > MA200（トレンド整列）
      2. MA200が上昇トレンド（20営業日前より高い）
      3. 現値 > MA50 > MA150 > MA200
      4. 52週安値から+25%以上
      5. 52週高値の75%以内（高値から-25%以内）
      6. RS（直近63日の対TOPIX相対パフォーマンス）が正

    Returns dict with sepa_stage, sepa_ma_align, sepa_ma200_trend,
                        sepa_from_low, sepa_from_high, sepa_rs
    """
    empty = {
        "sepa_stage": 0, "sepa_ma_align": False, "sepa_ma200_trend": False,
        "sepa_from_low": np.nan, "sepa_from_high": np.nan, "sepa_rs": np.nan,
    }
    if len(close) < 200:
        return empty

    ma50  = close.rolling(50,  min_periods=40).mean()
    ma150 = close.rolling(150, min_periods=120).mean()
    ma200 = close.rolling(200, min_periods=160).mean()

    c   = float(close.iloc[-1])
    m50 = float(ma50.iloc[-1])
    m150= float(ma150.iloc[-1])
    m200= float(ma200.iloc[-1])

    if any(np.isnan(v) for v in [c, m50, m150, m200]):
        return empty

    # 条件1: MAの整列
    ma_align = (m50 > m150 > m200)

    # 条件2: MA200が上昇トレンド（20営業日前より高い）
    ma200_20ago = float(ma200.iloc[-21]) if len(ma200) >= 21 and not np.isnan(ma200.iloc[-21]) else np.nan
    ma200_trend = (not np.isnan(ma200_20ago)) and (m200 > ma200_20ago)

    # 条件3: 現値 > MA50（MA150・200はma_alignで担保）
    above_all_ma = (c > m50) and ma_align

    # 条件4: 52週安値からの上昇率
    low_52w = float(close.iloc[-252:].min()) if len(close) >= 252 else float(close.min())
    from_low = (c - low_52w) / low_52w * 100 if low_52w > 0 else np.nan

    # 条件5: 52週高値からの下落率（-25%以内が望ましい）
    high_52w = float(close.iloc[-252:].max()) if len(close) >= 252 else float(close.max())
    from_high = (c - high_52w) / high_52w * 100 if high_52w > 0 else np.nan

    # 条件6: RS（直近63営業日の対TOPIX相対パフォーマンス）
    rs = np.nan
    if topix_close is not None and len(topix_close) >= 64:
        # 銘柄とTOPIXを同じ長さに揃える
        n = min(len(close), len(topix_close))
        stk = close.iloc[-n:].reset_index(drop=True)
        tpx = topix_close.iloc[-n:].reset_index(drop=True)
        if len(stk) >= 64 and float(stk.iloc[-64]) > 0 and float(tpx.iloc[-64]) > 0:
            stk_ret = (float(stk.iloc[-1]) / float(stk.iloc[-64]) - 1) * 100
            tpx_ret = (float(tpx.iloc[-1]) / float(tpx.iloc[-64]) - 1) * 100
            rs = round(stk_ret - tpx_ret, 1)

    # ステージ分類
    sepa_conditions = sum([
        ma_align,
        ma200_trend,
        above_all_ma,
        (not np.isnan(from_low))  and from_low  >= 25,
        (not np.isnan(from_high)) and from_high >= -25,
        (not np.isnan(rs))        and rs > 0,
    ])

    if above_all_ma and sepa_conditions >= 4:
        stage = 2  # 買いゾーン
    elif c > m200 and not ma_align:
        stage = 1  # 基盤形成中
    elif c < m200 and not ma_align and m50 < m150:
        stage = 4  # 下降トレンド
    else:
        stage = 3  # 天井圏・分配局面

    return {
        "sepa_stage"      : stage,
        "sepa_ma_align"   : ma_align,
        "sepa_ma200_trend": ma200_trend,
        "sepa_from_low"   : round(from_low,  1) if not np.isnan(from_low)  else np.nan,
        "sepa_from_high"  : round(from_high, 1) if not np.isnan(from_high) else np.nan,
        "sepa_rs"         : rs,
    }


def _calc_momentum_signals(cp, fins_df, code, topix_close=None, lookback=20, vol_mult=2.0):
    """直近lookback営業日以内のモメンタムシグナルを検出して辞書で返す。"""
    try:
        cp = cp.copy()
        cp["Date"] = pd.to_datetime(cp["Date"])
        cp = cp.sort_values("Date").reset_index(drop=True)
        if len(cp) < 60:
            return _empty_mom()

        close  = pd.to_numeric(cp["AdjC"],  errors="coerce")
        volume = pd.to_numeric(cp["AdjVo"], errors="coerce").fillna(0)

        ma5        = close.rolling(5,   min_periods=1).mean()
        ma25       = close.rolling(25,  min_periods=1).mean()
        ma200      = close.rolling(200, min_periods=1).mean()
        gc         = (ma5 > ma25) & (ma5.shift(1) <= ma25.shift(1))
        high_252   = close.shift(1).rolling(252, min_periods=60).max()
        new_high   = close > high_252
        avg_vol    = volume.shift(1).rolling(20, min_periods=10).mean()
        vol_flag   = volume > avg_vol * vol_mult
        vol_ratio  = volume / avg_vol.replace(0, np.nan)
        ema12      = close.ewm(span=12, adjust=False).mean()
        ema26      = close.ewm(span=26, adjust=False).mean()
        macd_line  = ema12 - ema26
        sig_line   = macd_line.ewm(span=9, adjust=False).mean()
        macd_cross = (macd_line > sig_line) & (macd_line.shift(1) <= sig_line.shift(1))
        above_ma200 = close > ma200
        ma200_ratio = (close / ma200.replace(0, np.nan) - 1) * 100

        other  = gc.astype(int) + new_high.astype(int) + macd_cross.astype(int)
        signal = vol_flag & (other >= 1) & above_ma200

        # SEPA評価
        sepa = _calc_sepa(close, topix_close)

        # ─── グランビル法則（買いパターン）─────────────────────────────
        # G1: MA200を下から上にクロス（直近20日以内）
        gran_g1 = bool(((close > ma200) & (close.shift(1) <= ma200.shift(1))).iloc[-20:].any())
        # G2: MA200付近（±5%）から上昇（直近10日以内）
        near_ma200 = (close >= ma200 * 0.95) & (close <= ma200 * 1.05)
        gran_g2    = bool((near_ma200 & (close > close.shift(1))).iloc[-10:].any())

        # ─── ダウ理論（トレンド確認）────────────────────────────────────
        # 20日ローリング高値・安値が20日前より高ければ上昇トレンド
        roll_high = close.rolling(10, min_periods=5).max()
        roll_low  = close.rolling(10, min_periods=5).min()
        dow_uptrend = (
            len(roll_high) >= 42
            and not np.isnan(roll_high.iloc[-1])
            and not np.isnan(roll_high.iloc[-21])
            and float(roll_high.iloc[-1]) > float(roll_high.iloc[-21])
            and float(roll_low.iloc[-1])  > float(roll_low.iloc[-21])
        )

        recent = signal.iloc[-lookback:]
        if not recent.any():
            result = _empty_mom()
            result.update(sepa)
            result.update({"gran_g1": gran_g1, "gran_g2": gran_g2, "dow_uptrend": dow_uptrend})
            return result

        si = recent[recent].index[-1]  # 最も新しいシグナルの位置（0始まり）

        vr = float(vol_ratio.iloc[si])
        mr = float(ma200_ratio.iloc[si])
        return {
            "mom_signal"      : True,
            "mom_signal_date" : cp.loc[si, "Date"].strftime("%Y/%m/%d"),
            "mom_signal_close": round(float(close.iloc[si]), 1),
            "mom_vol_ratio"   : round(vr, 1) if not np.isnan(vr) else np.nan,
            "mom_gc"          : bool(gc.iloc[si]),
            "mom_new_high"    : bool(new_high.iloc[si]),
            "mom_macd"        : bool(macd_cross.iloc[si]),
            "mom_above_ma200" : bool(above_ma200.iloc[si]),
            "mom_ma200_ratio" : round(mr, 1) if not np.isnan(mr) else np.nan,
            "mom_revision"    : _get_revision(fins_df, code),
            **sepa,
            "gran_g1"    : gran_g1,
            "gran_g2"    : gran_g2,
            "dow_uptrend": dow_uptrend,
        }
    except Exception:
        return _empty_mom()


# ─── メトリクス計算（APIコールなし・ローカル処理） ───────────────

def _compute_metrics(code, prices_df, fins_df, info_row):
    """1銘柄分のスコア・指標を計算して辞書で返す。データ不足は None。
    prices_df・fins_df はこのコード専用に事前フィルター済みの想定。"""
    try:
        cp = prices_df  # build_stock_cache でコード別グループ化済み
        if len(cp) < 20:
            return None

        cp    = cp.sort_values("Date")
        close  = pd.to_numeric(cp["AdjC"],  errors="coerce").dropna()
        volume = pd.to_numeric(cp["AdjVo"], errors="coerce").fillna(0)

        if len(close) < 20:
            return None

        latest_close  = float(close.iloc[-1])
        latest_volume = int(volume.iloc[-1])
        avg_vol = calc_avg_volume(volume)
        rsi     = calc_rsi(close)
        ma25    = calc_moving_average(close, 25)

        # 財務データ（build_stock_cache でコード別グループ化済み）
        cf = fins_df
        if cf.empty:
            return None
        cf = cf.sort_values("DiscDate", ascending=False).reset_index(drop=True)
        latest = cf.iloc[0]  # 最新レコード（Q3/Q2/FY いずれか）

        # FYレコード群（前期比較用）
        cf_fy = cf[cf["CurPerType"] == "FY"] if "CurPerType" in cf.columns else cf
        latest_fy = cf_fy.iloc[0] if not cf_fy.empty else latest

        # 株式分割対応: 最新開示日後のAdjFactor累積積で分割比率を取得
        disc_date    = pd.to_datetime(latest.get("DiscDate"), errors="coerce")
        cp_dates     = pd.to_datetime(cp["Date"], errors="coerce")
        cp_adj       = pd.to_numeric(cp["AdjFactor"], errors="coerce").fillna(1.0)
        split_factor = 1.0
        if pd.notna(disc_date):
            adj_after = cp_adj[cp_dates > disc_date]
            if len(adj_after) > 0:
                split_factor = float(adj_after.prod())

        # PER: FEPS（今期予想）→ NxFEPS（来期予想、FY確報済みの場合）→ 実績EPS の順で優先
        # FY確報発表後は FEPS が空になり NxFEPS に来期予想が入る
        # NxFEPS は開示時点で既に分割後ベースのため split_factor 不要
        feps   = pd.to_numeric(latest.get("FEPS"),   errors="coerce")
        nxfeps = pd.to_numeric(latest.get("NxFEPS"), errors="coerce")
        eps    = pd.to_numeric(latest_fy.get("EPS"),  errors="coerce")
        if pd.notna(feps) and feps > 0:
            per_base = feps * split_factor   # 四半期予想EPS（分割調整あり）
        elif pd.notna(nxfeps) and nxfeps > 0:
            per_base = nxfeps                # 来期予想EPS（開示時点で分割後ベース）
        elif pd.notna(eps) and eps > 0:
            per_base = eps * split_factor    # 実績EPS（フォールバック）
        else:
            per_base = np.nan
        per = latest_close / per_base if not np.isnan(per_base) else np.nan

        # PBR
        eq     = pd.to_numeric(latest.get("Eq"),      errors="coerce")
        sh_out = pd.to_numeric(latest.get("ShOutFY"), errors="coerce")
        if pd.notna(sh_out) and split_factor > 0:
            sh_out = sh_out / split_factor    # 分割後は株数増加
        bps = eq / sh_out if (pd.notna(eq) and pd.notna(sh_out) and sh_out > 0) else np.nan
        pbr = latest_close / bps if (not np.isnan(bps) and bps > 0) else np.nan

        # ROE: 今期予想NP(FNP)があれば予想ROE=FNP/現Eq、なければ実績ROE=実績NP/平均Eq（株探方式）
        fnp    = pd.to_numeric(latest.get("FNP"),   errors="coerce")
        nxfnp  = pd.to_numeric(latest.get("NxFNp"), errors="coerce")  # 成長率計算でも使用
        np_val = pd.to_numeric(latest_fy.get("NP"), errors="coerce")
        eq_fy  = pd.to_numeric(latest_fy.get("Eq"), errors="coerce")
        if pd.notna(fnp) and fnp != 0:
            # 四半期予想 → FNP / 現在の自己資本（予想ROE）
            roe = fnp / eq_fy * 100 if (pd.notna(eq_fy) and eq_fy > 0) else np.nan
        else:
            # FY確報済み → 実績NP / 平均自己資本（株探・Yahoo方式）
            eq_prev = pd.to_numeric(cf_fy.iloc[1].get("Eq"), errors="coerce") if len(cf_fy) >= 2 else np.nan
            avg_eq  = (eq_fy + eq_prev) / 2 if (pd.notna(eq_fy) and pd.notna(eq_prev)) else eq_fy
            roe = np_val / avg_eq * 100 if (pd.notna(np_val) and pd.notna(avg_eq) and avg_eq > 0) else np.nan

        # 配当利回り: FDivAnn（今期予想）→ NxFDivAnn（来期予想、FY確報済み）→ DivAnn（実績）
        # NxFDivAnn は開示時点で分割後ベースのため split_factor 不要
        fdivann   = pd.to_numeric(latest.get("FDivAnn"),   errors="coerce")
        nxfdivann = pd.to_numeric(latest.get("NxFDivAnn"), errors="coerce")
        divann    = pd.to_numeric(latest.get("DivAnn"),    errors="coerce")
        if pd.notna(fdivann) and fdivann > 0:
            div_ann = fdivann * split_factor
        elif pd.notna(nxfdivann) and nxfdivann > 0:
            div_ann = nxfdivann   # 既に分割後ベース
        elif pd.notna(divann):
            div_ann = divann * split_factor
        else:
            div_ann = np.nan
        div_yield = div_ann / latest_close * 100 if (not np.isnan(div_ann) and latest_close > 0) else np.nan

        # 成長率: 予想純利益・売上 vs 前期FY実績
        # FNP→NxFNp→最新FY実績 の順で優先（FY確報済みの場合はNxFNpを使用）
        revenue_growth = np.nan
        profit_growth  = np.nan
        if len(cf_fy) >= 2:
            prev_fy    = cf_fy.iloc[1]
            prev_np    = pd.to_numeric(prev_fy.get("NP"),    errors="coerce")
            prev_sales = pd.to_numeric(prev_fy.get("Sales"), errors="coerce")
            curr_np = (fnp   if (pd.notna(fnp)   and fnp   != 0) else
                       nxfnp if (pd.notna(nxfnp) and nxfnp != 0) else
                       pd.to_numeric(latest_fy.get("NP"), errors="coerce"))
            fsales     = pd.to_numeric(latest.get("FSales"),   errors="coerce")
            nxfsales   = pd.to_numeric(latest.get("NxFSales"), errors="coerce")
            curr_sales = (fsales   if pd.notna(fsales)   else
                          nxfsales if pd.notna(nxfsales) else
                          pd.to_numeric(latest_fy.get("Sales"), errors="coerce"))
            if pd.notna(prev_np) and prev_np != 0:
                profit_growth  = (curr_np - prev_np) / abs(prev_np) * 100
            if pd.notna(prev_sales) and prev_sales != 0:
                revenue_growth = (curr_sales - prev_sales) / abs(prev_sales) * 100

        # ─── Altman Z-score（近似版）────────────────────────────────────
        # 利用可能データで原式に近似: CFO→X1, NP→X2, OP→X3, Eq/負債→X4, Sales→X5
        # 日本株は原式より低く出るため参考値として保存（ハードフィルターは別途）
        ta_val  = pd.to_numeric(latest_fy.get("TA"),  errors="coerce")
        op_val  = pd.to_numeric(latest_fy.get("OP"),  errors="coerce")
        cfo_val = pd.to_numeric(latest_fy.get("CFO"), errors="coerce")
        altman_z = np.nan
        if pd.notna(ta_val) and ta_val > 0 and pd.notna(op_val) and pd.notna(np_val):
            sales_fy = pd.to_numeric(latest_fy.get("Sales"), errors="coerce")
            liab     = ta_val - (eq if pd.notna(eq) else 0)
            x3 = op_val  / ta_val
            x4 = eq / liab if (pd.notna(eq) and liab > 0) else np.nan
            x5 = sales_fy / ta_val if pd.notna(sales_fy) else np.nan
            x1 = cfo_val / ta_val  if pd.notna(cfo_val) else np.nan
            x2 = np_val  / ta_val
            z  = 3.3 * x3
            if pd.notna(x4): z += 0.6  * x4
            if pd.notna(x5): z += 1.0  * x5
            if pd.notna(x1): z += 1.2  * x1
            z += 1.4 * x2
            altman_z = round(z, 3)

        score = 0.0
        if not np.isnan(per) and per > 0:
            score += max(0, (20 - per) / 20 * 25)
        if not np.isnan(pbr):
            score += max(0, (1.5 - pbr) / 1.5 * 15)
        if not np.isnan(roe):
            score += min(roe / 20 * 20, 20)
        if not np.isnan(revenue_growth):
            score += min(revenue_growth / 20 * 20, 20)
        if not np.isnan(rsi):
            score += max(0, 10 - abs(rsi - 50) / 5)

        sig_score, sig_labels = calc_signal_score(close)
        mom = _calc_momentum_signals(cp, fins_df, code, topix_close=info_row.get("_topix_close") if info_row else None)

        return {
            "code"         : code,
            "code_4"       : code[:4],
            "close"        : round(latest_close, 1),
            "score"        : round(score, 1),
            "signal_score" : sig_score,
            "signals"      : ", ".join(sig_labels) if sig_labels else "−",
            "PER"          : round(per, 2)            if not np.isnan(per)            else np.nan,
            "PBR"          : round(pbr, 2)            if not np.isnan(pbr)            else np.nan,
            "ROE"          : round(roe, 2)            if not np.isnan(roe)            else np.nan,
            "div_yield"    : round(div_yield, 2)      if not np.isnan(div_yield)      else np.nan,
            "rev_growth"   : round(revenue_growth, 1) if not np.isnan(revenue_growth) else np.nan,
            "profit_growth": round(profit_growth, 1)  if not np.isnan(profit_growth)  else np.nan,
            "RSI"          : round(rsi, 1)            if not np.isnan(rsi)            else np.nan,
            "MA25"         : round(ma25, 1)           if not np.isnan(ma25)           else np.nan,
            "avg_volume"   : int(avg_vol),
            "latest_volume": latest_volume,
            "altman_z"     : altman_z,
            "op_positive"  : bool(pd.notna(op_val) and op_val > 0),
            "company_name" : info_row.get("CoName", "")                               if info_row else "",
            "market"       : info_row.get("MktNm", "")                                if info_row else "",
            "sector"       : info_row.get("S33Nm", info_row.get("S17Nm", ""))         if info_row else "",
            **mom,
        }
    except Exception:
        return None


def build_stock_cache(market_codes=None):
    """
    prices.parquet + fins_cache.parquet を読み込み、スコア・指標を計算して
    stock_cache.csv に保存する。APIコールは銘柄マスタ取得の1回のみ。

    Args:
        market_codes: 対象市場コードリスト（例: ["0111"]）。None なら全市場。
    Returns:
        pd.DataFrame
    """
    prices_df = _load_prices()
    fins_df   = _load_fins()

    if prices_df.empty or fins_df.empty:
        return pd.DataFrame()

    # 銘柄マスタ（1回のAPIコール）
    data       = _get("/equities/master")
    listed_df  = pd.DataFrame(data.get("data", []))

    if market_codes:
        listed_df = listed_df[listed_df["Mkt"].isin(market_codes)]

    codes    = listed_df["Code"].dropna().unique().tolist()
    info_map = {row["Code"]: row.to_dict() for _, row in listed_df.iterrows()}

    # TOPIX価格系列（RS計算用）: コード "13010" または "1306"（ETF代用）
    topix_close = None
    for topix_code in ["13060", "13010"]:
        tpx = prices_df[prices_df["Code"] == topix_code]
        if not tpx.empty:
            topix_close = pd.to_numeric(
                tpx.sort_values("Date")["AdjC"], errors="coerce"
            ).reset_index(drop=True)
            break

    # info_map にTOPIX系列を埋め込む（_compute_metrics経由で_calc_sepaに渡す）
    for code in codes:
        if code in info_map:
            info_map[code]["_topix_close"] = topix_close

    # コード別グループを事前構築（ループ内の全行フィルターを排除）
    # 直近300行に限定: MA200=200日・52W高安値=252日をカバーしつつデータ量を削減
    prices_grouped = {
        code: grp.sort_values("Date").tail(300).reset_index(drop=True)
        for code, grp in prices_df.groupby("Code")
    }
    fins_grouped = {
        code: grp.reset_index(drop=True)
        for code, grp in fins_df.groupby("Code")
    }

    results = []
    for code in codes:
        row = _compute_metrics(
            code,
            prices_grouped.get(code, pd.DataFrame()),
            fins_grouped.get(code, pd.DataFrame()),
            info_map.get(code),
        )
        if row:
            results.append(row)

    df = pd.DataFrame(results) if results else pd.DataFrame()
    if not df.empty:
        os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)
        df.to_parquet(CACHE_PATH, index=False)
    return df


# ─── 後方互換ラッパー ────────────────────────────────────────────

def fetch_all_stocks(market_codes=None, progress_callback=None):
    """
    後方互換API: 価格・財務データを更新してから stock_cache.csv を生成する。

    処理フロー:
      Phase1: update_prices()     → prices.parquet（バルク・差分）
      Phase2: update_fins()       → fins_cache.parquet（初回のみ全件、以降1コール）
      Phase3: build_stock_cache() → stock_cache.csv（ローカル計算）

    初回実行時は Phase2 で全銘柄の財務データを取得するため時間がかかります。
    2回目以降は Phase1・Phase2 ともに数コール程度で完了します。
    """
    fins_is_initial = not os.path.exists(FINS_PATH)

    # Phase1: 価格データ（バルク・差分）
    # 初回なら prices が約85コール、以降は1〜数コール
    price_weight = 0.2 if fins_is_initial else 0.9

    def price_cb(i, total, msg):
        if progress_callback:
            frac = (i + 1) / max(total, 1) * price_weight
            progress_callback(min(int(frac * 100), 99), 100, msg)

    update_prices(progress_callback=price_cb if progress_callback else None)

    # Phase2: 財務データ
    if fins_is_initial:
        def fins_cb(i, total, msg):
            if progress_callback:
                frac = price_weight + (i + 1) / max(total, 1) * (1.0 - price_weight - 0.05)
                progress_callback(min(int(frac * 100), 99), 100, msg)
        update_fins(progress_callback=fins_cb)
    else:
        update_fins()
        if progress_callback:
            progress_callback(95, 100, "📊 財務データ更新完了")

    # Phase3: メトリクス計算（APIコールなし）
    if progress_callback:
        progress_callback(96, 100, "⚡ スコア計算中...")

    return build_stock_cache(market_codes=market_codes)
