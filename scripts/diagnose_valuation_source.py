# -*- coding: utf-8 -*-
"""
バリュー株モード: 自前計算の PER/PBR/時価総額 と J-Quants バリュエーション指標の比較診断（2026-09-17）

比べる通り（同じ期間・同じ月末起点）:
  own_old : 自前計算。保有中に上場廃止した銘柄を集計から外す（旧集計方法。銘柄選びは own と同一）
  own     : 自前計算。保有中の上場廃止は最終取引価格で手放したとして集計
  jpx_ttm : PER / PBR / 時価総額を JPX 値に置き換え（PER は直近12ヶ月実績ベース）
  jpx_fwd : 同上で PER を会社予想ベース（FwdPER）に（本番パイプラインは予想 PER を使っている）
PSR は「時価総額 ÷ 本決算の売上」なので、JPX の時価総額に置き換えると PSR も変わる。

期間: JPX 値がある 2021-09 以降の月末で、forward_days 後の価格がある起点
出力: data/diagnose_value/valuation_source/<variant>_ic_fwd{N}.csv・<variant>_alpha_fwd{N}.csv・comparison_fwd{N}.csv

使い方（API 不要。Claude Code のシェルから実行可）:
  .venv/Scripts/python.exe scripts/diagnose_valuation_source.py --forward-days 250
  .venv/Scripts/python.exe scripts/diagnose_valuation_source.py --forward-days 250 --max-snapshots 2 --variants own
"""
import argparse
import os
import sys
import time

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)

import numpy as np
import pandas as pd

from services.diagnose_value_service import (
    calc_alpha_by_topn, calc_rank_ic, generate_monthly_snapshots, run_value_snapshot,
)
from services.pipeline_service import _load_fins_fy, _load_prices, _load_stock_cache

VALUATION_PATH = os.path.join(_ROOT, "data", "valuation.parquet")
OUT_DIR = os.path.join(_ROOT, "data", "diagnose_value", "valuation_source")
JPX_START = "2021-09-30"       # JPX 値（一括ダウンロード）の収録開始
# 既定の最初の起点。財務データは 2021-04 以降しかなく、ハードフィルタの売上成長率には前期の本決算が
# 要るため、3月決算の前期分が揃う 2022-05 より前は通過銘柄が数件しかない（2021-09-30 は 1 銘柄）
DEFAULT_START = "2022-06-30"
IC_FACTORS = ["PBR", "PER", "psr", "op_margin", "funda_score", "tech_score", "total_score"]
VARIANTS = {
    "own":     {"jpx": False, "per_basis": "ttm"},
    "jpx_ttm": {"jpx": True,  "per_basis": "ttm"},
    "jpx_fwd": {"jpx": True,  "per_basis": "forward"},
}


def _summarize(name, results, prices, forward_days, top_n):
    ic = calc_rank_ic(results, factors=IC_FACTORS)
    alpha = calc_alpha_by_topn(results, prices, forward_days, top_ns=[5, 10, 20, 30])
    ok = [r for r in results if "scored" in r]
    tops = [r["scored"][r["scored"]["has_fwd_data"] & (r["scored"]["rank"] <= top_n)] for r in ok]
    top = pd.concat(tops, ignore_index=True) if tops else pd.DataFrame(columns=["delisted", "return_pct"])
    delisted = top[top["delisted"].astype(bool)] if len(top) else top
    a = alpha.set_index("top_n").loc[top_n]
    ic_mean = ic.set_index("factor")["mean_ic"] if len(ic) else pd.Series(dtype=float)
    row = {
        "variant": name,
        "起点数": len(ok),
        "通過銘柄数_平均": round(float(np.mean([r["n_filtered"] for r in ok])), 0) if ok else np.nan,
        f"Top{top_n}_件数": a["n"],
        "平均リターン%": a["mean_return"],
        "中央値%": a["median_return"],
        "勝率%": a["win_rate"],
        "α_対TOPIX": a["alpha"],
        "α_対ユニバース": a["universe_alpha"],
        f"Top{top_n}内の上場廃止_件数": int(len(delisted)),
        f"Top{top_n}内の上場廃止_平均%": round(float(delisted["return_pct"].mean()), 1) if len(delisted) else np.nan,
    }
    for f in ["PBR", "PER", "psr", "funda_score", "total_score"]:
        row[f"IC_{f}"] = ic_mean.get(f, np.nan)
    return ic, alpha, row


def _save(name, ic, alpha, forward_days):
    ic.to_csv(os.path.join(OUT_DIR, f"{name}_ic_fwd{forward_days}.csv"), index=False, encoding="utf-8-sig")
    alpha.to_csv(os.path.join(OUT_DIR, f"{name}_alpha_fwd{forward_days}.csv"), index=False, encoding="utf-8-sig")


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    ap = argparse.ArgumentParser(description="自前計算と JPX バリュエーション指標でバリュー株モードを比較診断する")
    ap.add_argument("--forward-days", type=int, default=250)
    ap.add_argument("--top-n", type=int, default=20)
    ap.add_argument("--variants", default="own,jpx_ttm,jpx_fwd", help="カンマ区切り（own を含めると own_old も出す）")
    ap.add_argument("--max-snapshots", type=int, help="動作確認用に直近 N 起点だけ計算する")
    ap.add_argument("--start", default=DEFAULT_START,
                    help="最初の起点（YYYY-MM-DD）。前期の本決算が揃う前の起点は通過銘柄が極端に少なく成績を歪める")
    args = ap.parse_args()
    fwd, top_n = args.forward_days, args.top_n
    variants = [v.strip() for v in args.variants.split(",") if v.strip()]

    t0 = time.time()
    print("データ読み込み中...", flush=True)
    prices = _load_prices()
    prices["Date"] = pd.to_datetime(prices["Date"], errors="coerce")
    fins_fy = _load_fins_fy()
    meta = _load_stock_cache()
    valuation = pd.read_parquet(VALUATION_PATH, columns=["Date", "Code", "PER", "FwdPER", "PBR", "MktCap"])
    valuation["Date"] = pd.to_datetime(valuation["Date"])

    snaps = [s for s in generate_monthly_snapshots(prices["Date"].min(), prices["Date"].max(), fwd) if s >= max(JPX_START, args.start)]
    if args.max_snapshots:
        snaps = snaps[-args.max_snapshots:]
    print(f"起点 {len(snaps)} 件（{snaps[0]} 〜 {snaps[-1]}）/ 保有 {fwd} 日 / Top{top_n}"
          f" / 読み込み {time.time() - t0:.0f} 秒", flush=True)

    os.makedirs(OUT_DIR, exist_ok=True)
    rows = []
    for name in variants:
        cfg = VARIANTS[name]
        # 起点ごとの結果を保存し、再実行時は保存済みを読む（途中で止まっても続きから再開できる）
        cache_dir = os.path.join(OUT_DIR, "cache", f"fwd{fwd}_top{top_n}", name)
        os.makedirs(cache_dir, exist_ok=True)
        results = []
        for i, snap in enumerate(snaps):
            cache_path = os.path.join(cache_dir, f"{snap}.pkl")
            if os.path.exists(cache_path):
                r = pd.read_pickle(cache_path)
                source = "保存済み"
            else:
                r = run_value_snapshot(
                    snap, prices, fins_fy, meta, top_n=top_n, forward_days=fwd,
                    valuation=valuation if cfg["jpx"] else None, per_basis=cfg["per_basis"],
                )
                pd.to_pickle(r, cache_path)
                source = "計算"
            results.append(r)
            status = f"通過 {r['n_filtered']}" if "scored" in r else f"スキップ: {r.get('error')}"
            print(f"[{name}] {i + 1}/{len(snaps)} {snap} {status}・{source}（経過 {time.time() - t0:.0f} 秒）", flush=True)

        ic, alpha, row = _summarize(name, results, prices, fwd, top_n)
        _save(name, ic, alpha, fwd)
        rows.append(row)

        if name == "own":
            old = [dict(r, scored=r["scored"].assign(
                       has_fwd_data=r["scored"]["has_fwd_data"] & ~r["scored"]["delisted"].astype(bool)))
                   if "scored" in r else r for r in results]
            ic_o, alpha_o, row_o = _summarize("own_old", old, prices, fwd, top_n)
            _save("own_old", ic_o, alpha_o, fwd)
            rows.append(row_o)

    comp = pd.DataFrame(rows)
    comp.to_csv(os.path.join(OUT_DIR, f"comparison_fwd{fwd}.csv"), index=False, encoding="utf-8-sig")
    pd.set_option("display.width", 220)
    pd.set_option("display.unicode.east_asian_width", True)
    print("\n=== 比較（列 = 通り）===")
    print(comp.set_index("variant").T.to_string())
    print(f"\n保存先: {OUT_DIR}（合計 {time.time() - t0:.0f} 秒）")


if __name__ == "__main__":
    main()
