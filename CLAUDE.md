# stock_analysis — プロジェクト概要

> **🚨🚨 PDF 取扱いルール（最優先・例外なし）🚨🚨**
>
> **PDF ファイルを扱うときは絶対に `Read` ツールを使うな。** Read ツールは pdftoppm に依存し Windows でこの環境では動かない。さらに pypdf / pdftotext / pdfplumber も**日本語 PDF で壊れる**（ToUnicode CMap 不在 + Windows cryptography DLL 問題）。
>
> **必ず以下を実行**:
>
> ```bash
> .venv/Scripts/python.exe scripts/extract_pdf.py "<pdf path>"
> ```
>
> （PyMuPDF を使うラッパースクリプト。`.raw/pdfs/任天堂.pdf` 等も完璧に処理可能）
>
> - 出力: UTF-8 テキストが stdout に出る。`--save` を付けると同名 `.txt` も保存
> - 「PDF が壊れている / 読めない」と判断する前に、まずこのスクリプトを試すこと
> - スクリプトが無い場合: `git pull` してから上記を実行
>
> ---
>
> **🌟 マスター運用ルールは vault にあり**: `C:\Users\ryohei\iCloudDrive\iCloud~md~obsidian\LLM Wiki\CLAUDE.md`
>
> セッション開始時、**まずマスターを Read で読み込むこと**。マスターには以下が含まれる:
> - セッション開始時の必須手順（メモリ読込含む）
> - ユーザ基本情報（言語・環境）
> - プロジェクト一覧
> - LLM Wiki 運用ルール（PDF ingest・wikilinks 規約・PyMuPDF 等）
>
> このファイル（`stock_analysis/CLAUDE.md`）は stock_analysis 固有の技術詳細のみを記載する。

---

日本株のスクリーニング・分析・ポートフォリオ管理ツール。J-Quants API v2 を主データソースとし、TDnet で適時開示情報を補完する。Streamlit 製 Web アプリとして Streamlit Cloud にデプロイ済み。

---

## 環境

| 項目 | 内容 |
|------|------|
| Python | 3.9.1（`X \| Y` 型ヒント不可 → `Optional[X]` を使う） |
| 仮想環境 | `.venv/`（常に `.venv\Scripts\python.exe` を使用） |
| 依存パッケージ | `requirements.txt` 参照 |
| 認証情報 | `.env`（Git管理外） |
| デプロイ | Streamlit Cloud（stock-analysis-ryohei.streamlit.app） |

### .env キー
```
JQUANTS_API_KEY=...      # J-Quants v2 APIキー（x-api-key ヘッダー認証）
ANTHROPIC_API_KEY=...    # Claude API（Haiku-4-5）
JQUANTS_MAIL=...         # J-Quants ログインメール（通常不要）
JQUANTS_PASS=...         # J-Quants ログインパスワード（通常不要）
```

### Streamlit 起動
```bash
cd C:\Users\ryohei\stock_analysis
.venv\Scripts\streamlit.exe run app.py --server.port 8502
# → http://localhost:8502
```

---

## アーキテクチャ概要

### レイヤー構成

```
┌─────────────────────────────────────────────────────┐
│  UI Layer（pages/）                                 │
│  app.py  1_screening  2_stock_detail  3_disclosures │
│          4_portfolio  5_portfolio_analysis          │
└────────────────────┬────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────┐
│  Services Layer（services/）                        │
│  jquants_service   tdnet_service   batch_service    │
│  claude_service    ir_service      portfolio_service│
│                  screener.py（コアロジック）         │
└────┬──────────────────────────┬─────────────────────┘
     │                          │
┌────▼──────────┐    ┌──────────▼──────────────────────┐
│ External APIs │    │  Local Cache（data/）            │
│ J-Quants v2   │    │  stock_cache.csv                 │
│ TDnet         │    │  ir_summaries.json               │
│ Claude API    │    │  ai_analysis.json                │
└───────────────┘    │  backtest_prices/  fins_cache/   │
                     │  watchlist.json                  │
                     └──────────────────────────────────┘
```

---

## ファイル構成

```
stock_analysis/
├── app.py                        # Streamlitエントリーポイント・ホーム（5ページカード表示）
├── screener.py                   # スクリーニングコアロジック（CLI単独実行可）
├── backtest.py                   # モメンタム系シグナルのバックテスト
├── requirements.txt
├── CLAUDE.md                     # このファイル
├── .env                          # 認証情報（Git管理外）
│
├── pages/
│   ├── 1_screening.py            # スクリーニング（ファンダ×テクニカル / モメンタム戦略）
│   ├── 2_stock_detail.py         # 銘柄詳細（チャート・財務・適時開示タブ）
│   ├── 3_disclosures.py          # 適時開示（3タブ: 一覧 / AI要約フィルタ / 要約済み一覧）
│   ├── 4_portfolio.py            # ポートフォリオ（SBI CSV読み込み・損益・ヒートマップ）
│   ├── 6_trade_log.py            # トレードログ（実トレード記録・戦略別勝率集計）
│   ├── 7_pipeline_report.py      # パイプラインレポート（成長株/バリュー株モード切替）
│   ├── 8_backtest_value.py       # バリュー株モード バックテスト（クロスセクション）
│   ├── 9_sector_rotation.py      # セクター回転検知（温度・ランキング・信用需給3タブ）
│   └── 10_paper_trading.py       # ペーパー運用（バリュー株 Top10・仮想資金300万円）
│
├── services/
│   ├── jquants_service.py        # J-Quants API v2 ラッパー（@st.cache_data付き）
│   ├── tdnet_service.py          # TDnet Yanoshin API クライアント
│   ├── batch_service.py          # 全銘柄メトリクス一括取得・stock_cache.csv管理
│   ├── claude_service.py         # Claude API統合（ポートフォリオ分析・IR要約）
│   ├── ir_service.py             # 適時開示フィルタリング（3層分類）・PDF抽出
│   ├── portfolio_service.py      # SBI証券CSV パーサー（CP932デコード）
│   ├── pipeline_service.py       # パイプライン本体（ハードフィルタ→スコアリング→Claude分析）
│   ├── backtest_value_service.py # バリュー株バックテスト（過去スナップショット再現）
│   ├── rotation_service.py       # セクター回転（日次系列・出来高急増・鮮度・資金流入・温度）
│   ├── margin_service.py         # JPX週次信用残（PDF DL・パース・週次アーカイブ）
│   ├── paper_broker.py           # 仮想口座（現金・保有・注文・約定・分割換算。実運用では証券会社版に差し替える）
│   ├── value_portfolio.py        # バリュー株の1営業日の処理（月末入れ替え・上場廃止）
│   ├── value_paper.py            # ペーパー運用の追いつき処理・月末判定（データ更新後に自動実行）
│   └── breakout_*.py             # スイング戦略の検証用（不合格。再利用は検証のみ）
│
├── components/
│   ├── chart.py                  # Plotly OHLCVチャート（MA・BB・MACD・RSI・一目均衡表等）
│   ├── financial_cards.py        # 財務指標カード・スコアバッジ
│   └── disclosure_table.py       # 適時開示テーブル表示
│
├── data/
│   ├── stock_cache.csv           # 全銘柄スコア・指標キャッシュ（手動更新）
│   ├── watchlist.json            # ウォッチリスト（code_4/name/target_price/memo/added_at）
│   ├── ir_summaries.json         # IR AI要約キャッシュ（オンデマンド保存）
│   ├── ai_analysis.json          # ポートフォリオAI分析結果（オンデマンド保存）
│   ├── backtest_prices/          # バックテスト用OHLCVキャッシュ（銘柄別CSV）
│   ├── fins_cache/               # 決算データキャッシュ（銘柄別CSV）
│   ├── da_cache/                 # 深層分析ヘルパー出力JSON（Claude Codeが読む）
│   ├── margin/                   # JPX週次信用残アーカイブ（YYYYMMDD.parquet・Git管理外）
│   ├── backtest_records.csv      # バックテスト詳細トレード記録
│   └── backtest_summary.csv      # バックテスト集計統計
│
├── scripts/
│   ├── extract_pdf.py            # PDF テキスト抽出（PyMuPDF、日本語対応）
│   ├── deep_analysis_helper.py   # 深層分析用 J-Quants 株価+テクニカル取得
│   └── ...
│
└── styles/
    └── custom.css                # カスタムCSS（スマホ対応含む）
```

### 深層分析ヘルパー (scripts/deep_analysis_helper.py)

Claude Code の `deep-analysis-jp` スキルが使う J-Quants データ取得ヘルパー。**Streamlit パイプラインが既にメンテしている `data/prices.parquet` + `data/fins_cache.parquet` を直読み**するため、Claude Code Bash の SSL 制限を回避できる。

```bash
.venv/Scripts/python.exe scripts/deep_analysis_helper.py <ticker> --save
```

- ticker: 4桁数字（7974）/ 5桁（79740）/ 英数字混合（290A）いずれも可
- 出力: stdout + `--save` で `data/da_cache/<TICKER>.json`
- 内容: 株価・MA5/25/60/200・RSI・MACD・52週レンジ・出来高・分割イベント・配当履歴 5-7 期・四半期決算 8 件
- `services/split_adjust` で末尾日スケールに正規化済み（生 OHLC × cum_factor）

**データソース切替**:
- `--source local` (デフォルト): parquet 直読み。Claude Code Bash でそのまま動く
- `--source api`: J-Quants v2 直接呼び出し。ユーザのターミナル実行のみ。普段は local で十分

**鮮度管理**: 出力 JSON の `cache_info` で parquet 最終更新日を確認。3 営業日以上古ければ Streamlit で「データ更新」ボタン押下。

**重要設計判断**: `screener.JQuantsClient` は AdjC を Close にリネームして生 C を捨てるため流用不可。ヘルパー内では独自に `RawJQuantsClient`（api モード用）と `LocalParquetClient`（local モード用）を実装し、いずれも生 OHLC + AdjFactor を保持。

**sector_comparison 機能（2026-05-26 追加）**: `data/stock_cache.parquet`（3,781 銘柄 × 40 列の計算済み指標）を読み、自銘柄のセクター内 percentile・SEPA Stage 分布・モメンタムシグナル分布・同業 Top 5 を算出。深層分析の「主観的な過熱感」を「セクター内上位 X%」の客観数値に翻訳。local モード限定。

---

## 主要データフロー

### ① スクリーニング
```
1_screening.py
  →「🔄 データ更新」ボタン
  → batch_service.fetch_all_stocks()
     → J-Quants API（銘柄マスタ・株価・財務）
     → stock_cache.csv に保存
  →「▶ スクリーニング実行」ボタン
  → stock_cache.csv を読み込みフィルタリング → テーブル表示
```

### ② 適時開示AI要約
```
3_disclosures.py（Tab2）
  → tdnet_service → TDnet Yanoshin API
  → ir_service.classify_disclosures()（3層: 🔴必読/🟡推奨/🔵参考）
  → ir_service.fetch_pdf_text()（pypdfでPDFテキスト抽出）
  → claude_service.summarize_ir()（Claude API → ir_summaries.json保存）
```

### ③ ポートフォリオAI分析
```
5_portfolio_analysis.py
  → portfolio_service.parse_sbi_csv()（~/Downloads/New_file*.csv）
  → batch_service.load_cache()（stock_cache.csv）
  → claude_service.build_stock_context() → analyze_portfolio()
  → ai_analysis.json 保存 → 結果表示
```

---

## キャッシュ戦略

| キャッシュ | 形式 | 更新タイミング | 備考 |
|-----------|------|--------------|------|
| `stock_cache.csv` | CSV | 手動（🔄ボタン） | 全銘柄のスコア・指標 |
| `ir_summaries.json` | JSON | AI要約実行時（オンデマンド） | 鮮度管理なし（現状課題） |
| `ai_analysis.json` | JSON | AI分析実行時（オンデマンド） | 保存済みだが次回分析への自動注入なし（課題） |
| `backtest_prices/` | CSV群 | バックテスト実行時 | 銘柄別ファイル |
| `fins_cache/` | CSV群 | モメンタムスクリーナー更新時 | 決算データ全件上書き |
| `data/margin/` | parquet群 | セクター回転ページの信用データ更新ボタン | JPX公式PDFを週次アーカイブ・直近5週保持 |
| `@st.cache_data` | メモリ | セッション内 | APIレスポンスの一時キャッシュ |

---

## 外部連携サービス

| サービス | Base URL | 認証 | 用途 |
|---------|---------|------|------|
| J-Quants API v2 | `api.jquants.com/v2` | `JQUANTS_API_KEY` | 株価・財務・銘柄マスタ |
| TDnet Yanoshin | `webapi.yanoshin.jp` | 不要（パブリック） | 適時開示メタデータ |
| Claude API | `api.anthropic.com` | `ANTHROPIC_API_KEY` | AI分析・IR要約 |
| SBI証券CSV | ローカル | 不要 | ポートフォリオ保有データ |
| TDnet PDF | `document_url`経由 | 不要 | 決算短信PDF本文 |
| JPX 信用残 | `jpx.co.jp`（週次PDF） | 不要 | 銘柄別信用取引残高（セクター回転の需給） |

### Claude API 設定
- モデル: `claude-haiku-4-5-20251001`
- 料金: 入力 $0.80/MTok・出力 $4.00/MTok
- 月次上限: $10（console.anthropic.comで設定済み）

---

## バリュー株モード仕様（pipeline_service.py / 7_pipeline_report.py）

### ハードフィルタ（HARD_VALUE）
- PBR ≤ 1.5 / PER ≤ 25（かつ正値）
- 自己資本比率 ≥ 40%（倒産リスク管理）/ 営業黒字（キャッシュフロー安全）
- 時価総額 > 100億（流動性確保）/ 売上成長率 ≥ 3%
- ~~ROE ≥ 8%~~ **撤廃**（2026-04-29）: Rank IC 診断で IC=-0.027 と逆効果と判明
- ~~シクリカル5業種除外~~ **撤廃**（2026-04-29）: 分割バグ修正後の再検証で逆効果

### ファンダスコア（FUNDA_MAX_VALUE / 100点満点+ボーナス）
**【2026-04-29 改訂】Rank IC 診断（37スナップショット）に基づく PBR-heavy 設計**
- Value 90pt（PBR 50 / PSR 30 / PER 10）
- Quality 10pt（op_margin のみ）
- 撤廃: ROE / rev_growth / profit_growth / eps_growth / equity_ratio（IC≈0 または逆効果）
- ボーナス（経営変化シグナル / "PBR 改善 = 利益改善 × 経営の意志" の後者を捕捉）:
  - **アクティビスト保有 +10pt**（2026-04-30 追加 / EDINET DB MCP）
  - 2期連続増益 +10pt / 1期増益 +5pt（op_trend）
  - 2期連続増配 +10pt / 1期増配 +5pt（div_trend）
  - **【2026-06-14 撤廃】V字転換(+15pt) と 配当性向(40-70%+10/25-40%+5) は funda_score から除外**。
    分解診断（成分別IC + 1個ずつ抜く検証）で、Top20 では両者が逆効果（より割安な良銘柄を
    押しのける）と 250日先/60日先の両期間で確認。`bonus_turnaround`/`bonus_payout` 列は
    監視用に計算を残すが `bonus_total`（=funda_score 加算分）には含めない。
    → Top20 α: +9.4% → 約+11.2%（fwd250, in-sample）。診断 CSV: `data/diagnose_value/bonus_*.csv`

### 経営変化スコア（governance_score.py）
**【2026-04-30 新設】** ChatGPT-5 提案フレームワークに基づく経営の意志の数値化。
- データソース: `data/governance_activists.json` (EDINET DB MCP `get_activist_positions` バルク取得)
- 現状（MVP）: アクティビスト保有 +10pt のみ
  - カバレッジ: 全市場 442 銘柄が物言う株主に保有されている
  - フィルタ通過 358 銘柄中 39 銘柄 (10.9%) がアクティビスト保有
- 月次でリフレッシュ推奨（手動・ENV キーで EDINET DB API 直接取得も可能）

#### 拡張予定（Phase 2.5+）
- PBR 開示検出（search_ir_sections, theme_tag=fin:tse_capital_awareness）
- 自社株買い実績（get_events で時系列収集）
- 政策保有株削減（get_cross_shareholdings YoY 比較）
- ROIC/WACC 開示・中計 ROE/PBR 目標

各指標の Rank IC（37ヶ月平均、IC>0.10 で実用レベル）:
- PBR +0.151（37/37 月でプラス）/ PSR +0.143 / PER +0.082 ← 採用
- ROE -0.027 / rev_growth -0.017 / profit_growth -0.017 ← 撤廃

### テクニカルスコア（成長株モードと共通の関数 `_tech_score_single` で mode 分岐）

**バリューモード（100pt）**
- MA 30pt: MA200乖離率で評価（0〜+5%が30pt最高、+5〜+15%が20pt、+15%超は10pt、-5〜0%は15pt）
- RSI 20pt: 30-45ゾーンを最高評価（45-55は10pt、25-30は5pt）
- MACD 20pt / 出来高（5日/20日比） 15pt / 高値ブレイク 15pt

**成長株モード（100pt）**
- SEPA 30pt（Stage2=30 / Stage3=15 / Stage1=10 / Stage4=0）
- MA 20pt（close>MA25>MA60 のトレンド整列） / RSI 15pt（50-65 最高）
- MACD 15pt / 出来高（当日/25日比） 10pt / 高値ブレイク 10pt

### シグナル判定
- BUY: 総合 ≥ 60 / テクニカル ≥ 55 / RSI 30-50 / 株価 > MA25
- WATCH: 総合 ≥ 50（条件不足の場合）
- 利確: 第1目標 +35% / 第2目標 +40%
- 損切り: -15%（or MA25の高い方）
- **Top-N 選定は total_score 純順位**（`select_top_candidates`）。シグナル順
  （BUY→WATCH→AVOID）の並べ替えは表示用であり選定には使わない
  （バックテストの検証が total_score 順位ベースのため。2026-06-12 修正）
- **Top-N: バリュー=20 / 成長株=5**（成長株は 2026-06-14 に 10→5 へ。α が上位3-5に
  集中し Top7 以降で急減衰するため。下記 de-biased 診断参照）

### バックテスト検証実績

#### 【2026-09-17 再計測】上場廃止の扱い修正・一括ダウンロード後・JPX 値との比較（fwd 250日 × 43 月次）

期間 2022-06〜2025-12 の月末起点（財務データが 2021-04 以降しか無く、前期の本決算が揃う前の起点は
通過銘柄が数件しかないため除外）。`scripts/diagnose_valuation_source.py`、結果は `data/diagnose_value/valuation_source/`。

| 通り | Top20 α（対TOPIX） | α（対ユニバース） | 勝率 | IC funda | IC PER |
|---|---|---|---|---|---|
| 自前計算・旧集計（保有中の上場廃止を除外） | +11.0% | +10.2% | 80.8% | +0.162 | −0.099 |
| **自前計算（上場廃止を最終価格で集計）** | **+11.4%** | +10.4% | 81.0% | +0.161 | −0.098 |
| JPX 値（PER は直近12ヶ月実績） | +11.1% | +10.3% | 81.7% | +0.159 | −0.091 |
| JPX 値（PER は会社予想） | +10.9% | +10.0% | 80.7% | +0.160 | **−0.125** |

- **自前計算と JPX 値で成績は変わらない**: 起点ごとの差は −0.3〜−0.5pt（素朴な t ≦ 1.0、起点の重なりを考えるとさらに小さい）。
  43 起点中 36 起点でプラス、年別 α は 2022: +14 / 2023: +10 / 2024: +12〜13 / 2025: +8〜10%。JPX 値のほうが起点ごとのばらつきが小さい（標準偏差 10.2 対 11.2）
- 予想ベースの PER は実績ベースより IC が強い（−0.125 対 −0.09。PER は低いほど良いので負が正しい向き）
- 上場廃止の扱いの修正は Top20 では小さい（860 件中 11 件、その平均 +52.6%）。向きは上方
- 注意: 月次起点で 250 日保有なので隣接起点の成績は大きく重なる（実効的な標本は約 4 年分）。アクティビスト加点は現在の保有情報を使う（先読み）

#### 【2026-06-12 再計測】fins レコード選別バグ修正後（fwd 250日 × 52 月次スナップショット）

CurPerType=='FY' に混入していた予想修正レコードの除外バグ（J-Quants リファレンスの⚠️参照）を
修正し、ユニバースが約 +76%（時点あたり約350→500-670銘柄）拡大した状態で再計測:

| 指標 | Top20（現行設定） |
|---|---|
| 平均リターン | +22.2% |
| **α (vs TOPIX ETF)** | **+9.4%** |
| α (vs universe) | +8.0% |
| 勝率 | 76.5% |
| サンプル | 847 銘柄・スナップショット |

- 修正前の直近計測（2026-05-03, 同条件 fwd250）: mean +21.2% / α +8.97% / 勝率 78.7%
  → **α はユニバース拡大後も維持**（データバグは α の源泉ではなかった）
- funda_score IC **+0.179**（38/42 月でプラス, t=+11.9）→ PBR-heavy 設計は引き続き支持
- 修正後は eps_growth IC +0.042 / profit_growth +0.039 と**わずかに正へ転じた**
  （旧計測の -0.017 はデータバグで前期比較が壊れていた影響を含む可能性）。
  ただし実用レベル（0.10）未満のため配点は変更しない
- 重みスイープ: funda 1.0 が α 最大（+9.9%）だが勝率は funda 0.4-0.5 が最良。現行 0.6/0.4 は妥当圏
- Top3〜30 で α は +9〜10% でほぼフラット → Top20 は分散と α のバランスとして妥当

#### 【2026-06-14】成長株モード de-biased 全期間診断（Spearman タイ対応修正後）

成長株モードは「短期 × 超集中」戦略。単独の系統的 α エンジンではなく**飛躍分析への
候補供給**が本務、とクリーンな全期間データ（fwd60: 57/60 / fwd250: 51/54 スナップショット）で確認:
- **α は上位3-5にほぼ全部**。fwd60 対TOPIX: Top3 **+5.5%** / Top5 +4.1% / Top7 +1.9% / Top20 +1.1% / Top30 −0.1%
- fwd250（1年保有）では Top20 **−1.6%**、tech は逆効果（モメンタムの反落）。**成長株は短期向け**
- 効く因子は tech_score（SEPA）のみ: fwd60 IC **+0.032**（t=1.99）。旧主張 +0.0479 はタイ・バグの水増し
- funda(成長) IC ≈ 0、成長率系（rev_growth −0.041 / ROE −0.042 / 直近Q売上 −0.065）は両期間でマイナス
  = **成長率パラドックス確定**（成長率を買い材料にするな。バリューでも成長株でも一貫）
- 成長株ユニバース内でも割安（低 PBR/PER）が効く → PEG 的な値段規律は妥当
- 重み funda0.6/tech0.4 は fwd60 で妥当圏（tech 単独・funda 単独はどちらも劣る）
- → 成長株 `top_n` を 10→5 に変更。診断 CSV: `data/diagnose_growth/`（fwd60/fwd250 で上書き運用）

#### 【2026-09-18】成長株モード: 上場廃止修正後・JPX 値との比較（2022-06〜、月次起点）

`scripts/diagnose_valuation_source.py --mode growth`、結果は `data/diagnose_growth/valuation_source/`。

| | α 対TOPIX（自前 / JPX予想） | 勝率（自前 / JPX） | 起点 |
|---|---|---|---|
| 60日・Top3 | +5.9% / +4.2% | — | 49 |
| 60日・Top5 | +3.6% / +2.6% | 54.3% / 52.7% | 49 |
| 250日・Top5 | +2.0% / +1.2% | 58.1% / 55.8% | 43 |

- JPX 値はどの比較でもわずかに低いが、起点ごとの差の t は −0.35〜−0.76 で**ばらつきの範囲**
  （Top5 の銘柄は 64% が共通。4 つの比較は独立ではない）。定義統一とバグ回避を優先して JPX 値に切り替えた
- 上場廃止の扱いの修正は成長株 Top5 ではほぼ影響なし（60日で 0 件、250日で 1 件）
- IC（60日）: tech_score +0.014〜0.023（6月の +0.032 より弱い）、funda_score ≈ 0、
  成長率は引き続きマイナス（rev_growth −0.04）= **成長率パラドックスは継続**
- IC（250日）: PER −0.19〜−0.21 / PBR −0.19〜−0.22 と強い = **成長株の中でも割安なものが効く**（PEG 的な値段規律を支持）
- α 対ユニバースは 60日 +4〜5% / 250日 +9% と対TOPIX より大きい（成長株ユニバース自体が TOPIX に負けている）

#### 【2026-09-22】スイング戦略は不合格・バリュー株 Top10 をペーパー運用へ

資金 300 万円の口座として日々の売買を再現するバックテスト（`services/paper_broker.py` + 1 営業日の処理）。
- **スイング（TradingView 系テクニカル）は全滅**（前半 2022-02〜2024-12、TOPIX 年 +14.3%）:
  ブレイクアウト順張り 年 +4.5%・PF 1.15、押し目買い A/B(+割安)/C(+上方修正) 年 +6.3/+8.2/+8.4%。
  1 取引あたりの対 TOPIX 超過は t = 1.2〜1.5 で偶然と区別できない。割安・上方修正を足すと取引の質は上がる（PF 1.22→1.30→1.71）が、
  現金が遊ぶ（C は投下率 37%）。過去のモメンタム系検証（`data/backtest_summary.csv`、20 日後 +0.4〜1.7% ≒ 相場全体）と同じ結論
  = **値動きの形だけのシグナルに優位性は無い**。上方修正（予想純利益 +20% 以上）は唯一目立つが単独では弱い。
  `scripts/breakout_backtest.py`（`--strategy pullback`）、結果は `data/breakout_backtest/`
- **バリュー株を口座で運用**（`scripts/value_portfolio_backtest.py`、2022-07〜2026-09、TOPIX 年 +20.0%）:
  Top20・S株・40 位で売る 年 +35.6% / **Top10・S株・20 位で売る 年 +42.9%（採用）** / Top10・100 株単位 年 +35.1%（資金の約 2 割が現金）。
  5 年すべて TOPIX 超え、β 0.71、最大 DD −21.8%（2024-08-05。同期間 TOPIX −23.4%）。Top10 は Top20 を月 +0.60% 上回る（t = 2.89）。
  スコア設計が同じ期間を見て決められているので、**実運用の期待上乗せは年 +5〜10%** に割り引く
- ペーパー運用: `services/value_paper.py` / `pages/10_paper_trading.py`、状態は `data/paper/value_top10.json`（2026-09-18 開始）。
  「データ更新」の後に自動で最新日まで進む。実運用への移行は最低 3 か月かつ 20 取引の後
- 設計書: `docs/superpowers/specs/2026-09-22-breakout-paper-trading-design.md`・`2026-09-22-value-paper-trading-design.md`

#### 参考【旧数値・2026-04-29 計測】（37スナップショット × 365日 fwd、データバグ込み）
旧仕様 Top50 α+1.64% → PBR-heavy Top20 α+18.14%/勝率86.1% の比較で現行設計を決定した。
この絶対値は「ユニバース2割欠落 + 365日fwd + 生存バイアス」を含むため、現在は参照値扱い。

#### ⚠️ 期待値の現実調整
【2026-09-17 訂正】「廃止銘柄が prices.parquet に未収録」は誤り。prices.parquet には上場廃止銘柄の株価も入っていた。
実際の偏りは (1) 旧 fins_cache に上場廃止銘柄の開示が欠けていたため、その後に上場廃止する会社の8割以上が
過去時点の候補から抜けていた（一括ダウンロードの取り込みで解消）、(2) 保有中に上場廃止した銘柄を集計から外していた
（`services/forward_returns.py` で修正）の2点。上場廃止の大半は TOB・MBO でプラスに終わる（250 日以内の上場廃止は
平均 +18.3%、PBR 1 倍以下は中央値 +22.7%）ため、**偏りはむしろバリュー株の成績を低めに見せる向き**だった。
それでも in-sample・期間の重なり・2022〜2025 年のバリュー相場（東証の PBR 要請）を考えると、
実運用での期待 α は **+5〜10%/年**、勝率 70% 前後と控えめに見るのが妥当。

#### 主要診断結果（旧37ヶ月 backtest より・定性的には引き続き有効）
- **Top10 銘柄の β = 0.997** = TOPIX とほぼ同振幅。R² = 0.86 → リターンの 86% は TOPIX で説明
- セクター集中: 自動車・機械・卸売業で 41%（東証 PBR 要請ターゲット）
- 実質 low-turnover（同じ銘柄群を回し続ける）

---

## screener.py の構造

### クラス

**`JQuantsClient`**
- `x-api-key` ヘッダー認証で J-Quants v2 に接続
- `get_listed_info()` — 銘柄マスタ取得
- `get_daily_quotes(code, from_date, to_date)` — 日次株価（AdjC/AdjVo使用）
- `get_financials(code)` — 財務サマリー（FYのみフィルタ）

**`ScreeningCriteria`** (dataclass)
| フィールド | デフォルト | 説明 |
|-----------|-----------|------|
| `per_max` | 20.0 | PER上限 |
| `pbr_max` | 1.5 | PBR上限 |
| `pbr_min` | 0.5 | PBR下限 |
| `dividend_yield_min` | 2.0 | 配当利回り下限(%) |
| `revenue_growth_min` | 5.0 | 売上成長率下限(%) |
| `profit_growth_min` | 5.0 | 利益成長率下限(%) |
| `roe_min` | 8.0 | ROE下限(%) |
| `rsi_min` / `rsi_max` | 40.0 / 70.0 | RSI範囲 |
| `above_ma25` | True | 25日MA上であること |
| `volume_avg_min` | 100000 | 20日平均出来高下限 |

**スコア算式**（最大100点）
```python
score += max(0, (20 - per) / 20 * 25)    # PER: 低いほど高得点
score += max(0, (1.5 - pbr) / 1.5 * 15)  # PBR: 低いほど高得点
score += min(roe / 20 * 20, 20)           # ROE: 高いほど高得点（上限20点）
score += min(rev_growth / 20 * 20, 20)    # 売上成長: 高いほど高得点（上限20点）
score += max(0, 10 - abs(rsi - 50) / 5)  # RSI: 50に近いほど高得点
```

---

## J-Quants API v2 リファレンス

### エンドポイント

| エンドポイント | 説明 | 主なパラメータ |
|--------------|------|--------------|
| `GET /equities/master` | 上場銘柄マスタ（約4443件） | なし |
| `GET /equities/bars/daily` | 日次OHLCV（権利修正済含む） | `code`, `from`, `to` |
| `GET /fins/summary` | 財務サマリー（決算短信ベース） | `code` |

### equities/master 主要カラム
- `Code` — 銘柄コード（**5桁**。東証4桁コードの末尾に `0` を付与）
- `CoName` / `CoNameEn` — 会社名（日本語 / 英語）
- `MktNm` — 市場名（値は **"プライム" / "スタンダード" / "グロース"**。"東証"プレフィックスなし）
- `S17Nm` / `S33Nm` — セクター分類

### equities/bars/daily 主要カラム
`Date, Code, O, H, L, C, Vo, Va, AdjFactor, AdjO, AdjH, AdjL, AdjC, AdjVo`

#### ⚠️ 株式分割対応の重要な注意

**`AdjC`/`AdjVo` をそのまま使ってはいけない**。理由:
- J-Quants の `AdjC` は **API を叩いた時点でのスナップショット値**
- 当アプリは日次インクリメンタル取得のため、`prices.parquet` には「分割発生前に取得された未調整 AdjC」と「分割発生後に取得された調整済み AdjC」が混在する
- 結果、長期 MA・RSI 等を `AdjC` ベースで計算すると**スケール混在で破綻する**

**正しい方針**: `services/split_adjust.py` の helpers を使う。
- `normalize_close(cp)`: 生 `C` × cum_factor で末尾日スケールに統一した close 系列
- `normalize_volume(cp)`: 同じく出来高（株数で逆方向に分割反応）
- `split_factor_between(cp, from_date, to_date)`: 区間内の AdjFactor 累積積。
  per-share 値（EPS/BPS/DPS）の異時点比較・スケール変換に使う

**per-share 値と price を組み合わせる箇所すべてで、両者のスケールを揃えること**。
- PER = close / EPS → EPS を close と同じ日付スケールに変換する必要あり
- eps_growth = (cur - prev) / prev → 双方を共通スケールに正規化してから比較
- ShOutFY → 末尾日スケールに割り戻し（× shares で逆方向）

CLAUDE.md/コードに散らばっていた「権利修正済: AdjO/H/L/C/Vo を使用」の記述は **単発スナップショット利用時のみ妥当**。長期蓄積データには当てはまらない。

#### ⚠️ 予想 EPS/配当の分割スケール二重調整の罠（2026-06-13 修正）

**「開示日 < 分割日 → 予想は分割前ベース → split_factor を掛ける」という日付ルールは
予想 per-share 値（FEPS/FDivAnn）には使えない。** 分割発表後・効力前に開示された
通期予想は、会社により **分割後ベースで開示される**ことがある（実績 EPS と違い、予想は
将来の分割を織り込んで開示してよいため）。

実例: **6227 ＡＩメカテック**は 1:3 分割（効力 2026-03-30）の発表後、2026-02-13 の
2Q 短信で通期予想 EPS=163.93 を **分割後ベース**で開示（検算: 予想NP 3,078百万円 ÷
分割後株数 18.8M ≈ 163）。日付ルールで ÷3 すると予想 PER が 47.9 → 143.7 と 3 倍過大になる。
直近1年に分割があった 289 銘柄中 5 銘柄が同症状だった（いずれも分割後ベース開示）。

**対策**: `services/split_adjust.forecast_per_share_multiplier(per_share, aggregate, shares, split_factor)`
で **FNP/ShOutFY との突合**でスケールを判定してから乗数を決める:
- `FEPS ≈ FNP/shares` → 分割前ベース → 乗数 = split_factor
- `FEPS ≈ FNP/shares × split_factor` → 分割後ベース → 乗数 = 1.0（調整しない）
- 判定不能（分割なし / FNP・shares 欠損 / 両候補から乖離）→ split_factor にフォールバック

予想 EPS と予想配当は同一レコード＝同一ベースなので、EPS から得た乗数を配当にも流用する。
適用箇所: `batch_service._compute_metrics`（stock_cache の PER/利回り）と
`deep_analysis_helper.compute_valuation_estimate`（深層分析の予想 PER/利回り）。
**実績 EPS/DivAnn の異時点比較（pipeline_service の成長率・増配トレンド）は従来どおり
split_factor 適用で正しい**（完了済み期の実績は遡及修正されないため）。回帰テストは
`tests/test_forecast_split_scale.py`。

### fins/summary 主要カラム
- 実績: `Sales, OP, NP, EPS, BPS, Eq, TA, CFO`（単位: **円**。EPS/BPS は円/株）
- 配当: `DivAnn, FDivAnn`（予想年間配当）
- 予想: `FSales, FOP, FNP, FEPS`（今期予想）
- `CurPerType` — `FY`（通期） / `1Q`〜`3Q`（四半期）
- `DiscDate` — 開示日 / `DocType` — 文書種別 / `CurFYEn` — 決算期末

#### ⚠️ CurPerType=='FY' フィルタの罠（2026-06-12 修正）

`CurPerType=='FY'` は「FY 期間に関する開示」であって**決算短信とは限らない**。
業績予想修正（`EarnForecastRevision`）・配当予想修正（`DividendForecastRevision`）も
`CurPerType=='FY'` を持ち、これらは**実績列（Sales/OP/EPS/Eq/ShOutFY）がすべて空**。

修正前の実測影響: 最新 FY レコードが予想修正の銘柄が 757/3,785（20%）あり、
ハードフィルタで無条件除外されていた（増配修正を出した会社ほど除外される逆選択）。
さらに訂正短信による同一決算期の重複が 229 銘柄で前期比較を破壊していた。

**対策（`services/fins_utils.py`）**:
- `filter_fy_statements(df)`: 実績を読む場面では DocType が
  `FYFinancialStatements*` のレコードに限定する
- `dedupe_same_fy(df)`: 同一 (Code, CurFYEn) は DiscDate 最新のみ残す。
  **バックテストでは as_of フィルタ後に適用**（ロード時に dedup すると
  未来の訂正が原本を消し point-in-time 性が壊れる）
- 予想列（FEPS/FDivAnn 等）を読む場面では予想修正レコードが最新情報を
  持つため、フィルタせず最新レコード（`latest`）から読む（batch_service 方式）

#### ⚠️ fins_cache の取りこぼしと成長率の比較年度ずれ（2026-09-15 修正）

**① 差分取得の取りこぼし**: `update_fins()` は「当日の開示」1日分しか取得しておらず、
データ更新ボタンを押さなかった日の開示が永久に欠落していた（2026-04-16〜09-15 で
開示ゼロの取引日が約90日。prices は日付ループなので正常）。J-Quants のプラン遅延ではない。
- 修正後: 全開示を取得できた最終日を `data/fins_fetch_state.json`（`verified_through`）に記録し、
  次回はその 2 取引日前から今日までの全取引日を `/fins/summary?date=` で取得（`pagination_key` も辿る）
- 状態ファイルが無い初回は、既存キャッシュの「開示ゼロの取引日」の密集区間を推定してバックフィル
- 再発検知: パイプラインレポートに開示ゼロ取引日の警告と、銘柄別の最新決算短信日・未収録の警告を表示
  （`fins_utils.disclosure_freshness` / `find_disclosure_gaps`）
  - 【2026-09-19〜】未収録の判定は**決算発表予定日**（`data/earnings_dates.parquet`、/fins/earnings-date）で行う:
    予定日の翌日を過ぎても同じ決算区分の決算短信が無ければ未収録（予定変更は最後の公表を使う、
    前倒し開示は30日まで許容、予定日が空欄＝未定は使わない）。予定の無い銘柄だけ従来の推定
    「期末 + 3ヶ月 + 50日（REIT 等は +6ヶ月）」。決算短信が1件も無い銘柄（インフラファンド等）は判定しない
  - 予定日データはデータ更新ボタンで公表日ごとに差分取得（`batch_service.update_earnings_dates`）、
    過去分は一括ダウンロード。スコアカードに「次の決算予定」も表示
  - 検証: 決算データを 2026-05-17 までに切り詰めて 9/15 時点で判定すると、7803/5254/4417/135A/325A を
    実際の開示予定日（8/13・8/6・7/31・7/14・7/15）付きで検出。完全なデータでは未収録 26 → 13 銘柄（ニデック等の開示遅延）

**② 成長率の比較年度ずれ**: `_compute_metrics` の rev_growth / profit_growth は
「予想値（FNP/FSales または NxF*）÷ `cf_fy.iloc[1]`（最新確定FYのさらに1期前）」で、
**2 年分の伸び**を出していた（7803 利益 +586%、135A 売上 +125% など）。
修正後は `_growth_vs_prior_fy` で「予想の対象年度の直前に終わった FY 実績」と比較する。
**stock_cache を再構築するまで rev_growth / profit_growth は旧値のまま**。
なおバックテスト系（backtest_value_service / diagnose_*）は実績 FY 同士の比較で、このずれは無い。

**③ 変則決算期**: 決算期変更で期間長が 5% 超違う FY 同士は、今期のフロー値に
「前期日数 / 今期日数」を掛けて揃える（`fins_utils.period_length_scale`。325A の 7ヶ月決算など）。
REIT の 6ヶ月決算同士は補正しない。適用: 成長率・eps_growth・op_trend（div_trend は対象外）。
回帰テスト: `tests/test_fins_freshness.py`。

#### バリュエーション指標 API（/v2/equities/valuation、2026-09-15 導入）

J-Quants が決算短信と株価から日次で算出する EPS（実績=直近12ヶ月 / 予想=進行期の会社予想）・
BPS・ROE・PER・PBR・時価総額（**自己株控除後の株数**、百万円）。全プラン対応、日次 16:30 頃更新、
決算短信は開示の翌営業日から反映。ROE は小数（0.231 = 23.1%）。売上・配当は含まない。
- 取得: `batch_service.update_valuation()` → `data/valuation.parquet`（データ更新ボタンで実行。
  株価と同じ「最終日の翌日〜今日」の日付ループ。失敗日で止めて次回そこから取り直す。初回は直近45日）
- **【2026-09-18〜】本番の PER / PBR / ROE / 時価総額は JPX 値（PER・ROE は会社予想ベース）**。
  `build_stock_cache` が `services/valuation_source.apply_live_valuation` で上書きし、自前計算は
  `PER_self` / `PBR_self` / `ROE_self` に残す。PSR は「JPX 時価総額 ÷ 本決算の売上」。
  会社予想が無い銘柄は PER が空欄（約420銘柄）→ バリュー株モードのハードフィルタで除外される。
  valuation.parquet が空なら自前計算のまま（レポートに出所を表示）
- 切り替えの根拠: 答え合わせで自前計算に4種類のバグ（分割予定の予想EPS・予想修正で予想EPSを見失う・
  純資産ベースのPBR/ROE・本決算時点の株数での時価総額）。JPX 値のバックテストはバリュー株で同等
  （下記「2026-09-17 再計測」）、成長株でも差はばらつきの範囲（下記「2026-09-18」）
- 答え合わせは継続可能: `scripts/audit_valuation.py` が `*_self` 列と JPX 値を突き合わせ、ずれを
  「自己株の定義差 / 自己資本の時点差 / 分割の調整ずれ / 反映日差 / 要調査」に分類（`services/valuation_audit.py`）

#### 一括ダウンロード（/bulk/list・/bulk/get、Light 可、2026-09-16 導入）

- 同期: `scripts/jquants_bulk.py`（**利用者のターミナルで実行**。Claude Code のシェルからは API 不可）→ `data/bulk/<Key>`。
  既定はバリュエーション指標・財務情報・決算発表予定日の全期間（Light は約5年、216 ファイル）。
  `data/bulk/manifest.json` の LastModified・Size と比べ、更新されたファイルだけ取り直す
  （J-Quants は訂正を上書きで反映し差分を提供しない → **定期的に再実行すると訂正を拾える**）
- 取り込み: `scripts/import_bulk.py`（API 不要。Claude Code からも実行可）
  - 財務情報 → `fins_cache.parquet` に DiscNo 単位で統合（一括ダウンロード側を優先。書き込み前に `fins_cache.backup_*.parquet` を作成）
  - バリュエーション指標 → `valuation.parquet`（(Date, Code) 重複は新しいファイル優先）
  - 決算発表予定日 → `earnings_dates.parquet`（予定変更は新しい行として追加される仕様）
- 実ファイルで確認した事実（2026-09-16）:
  - 過去分は `historical/YYYY/<name>_YYYYMM.csv.gz`（月次）、**当月分は `live/<name>_YYYYMMDD.csv.gz`（日次）**
  - CSV の列名は API の項目名と同一、空欄は空文字（fins_cache と同じ形）
  - 財務情報の自己資本 `ShEq` は一括ダウンロードの全期間で約99%収録（API 差分取得分だけ見ると 2026-08 以降しか無かった）
  - 一括ダウンロードの財務情報は 2026-03 以前で旧 fins_cache より月約5%多かった（初回の銘柄別全件取得で、その後に上場廃止した銘柄の開示が欠けていたとみられる）。取り込み後、2021-09 以降の開示ゼロ取引日は大納会の2日のみ
  - バリュエーション指標の過去分には最新日に存在しない銘柄コードが 586 あり、**すべて prices.parquet にも存在した**。
    上記「生存バイアス（廃止銘柄が prices.parquet に未収録）」は現在の prices.parquet には当てはまらない可能性がある（未検証）

---

## TDnet Yanoshin API（アプリ用）

- URL: `https://webapi.yanoshin.jp/webapi/tdnet/list/{YYYYMMDD}.json`
- パラメータ: `limit`, `company_code`（4桁）
- レスポンス: `{"total_count": N, "items": [{"Tdnet": {id, pubdate, company_code, company_name, title, document_url, markets_string}}]}`
- **注意**: `company_code` は **4桁**（J-Quantsの5桁コードとは異なる。突合時は末尾`0`を除去）
- **注意**: `markets_string` は "東" 等の略号。"プライム" は含まれない（プライム判定は `stock_cache.csv` で代替）

---

## 注意事項

- `.env` は `.gitignore` で除外済み。APIキーをコードにハードコードしない。
- `load_dotenv()` は `-c` フラグ実行時に問題が出る場合があるため `load_dotenv('.env')` と明示する。
- Rate limit (429) 時は60秒待機してリトライ（`JQuantsClient._get` に実装済み）。
- PDF抽出ライブラリは `pypdf`（`pdfminer.six` はPython3.9.1+WindowsでcryptographyのDLLエラーが発生するため不使用）。
- PCとStreamlit Cloud は別サーバーのため `data/` ディレクトリは共有されない。
