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
│   ├── 5_portfolio_analysis.py   # AI分析（Claude APIによる総評・銘柄別売買提案）
│   ├── 7_pipeline_report.py      # パイプラインレポート（成長株/バリュー株モード切替）
│   └── 8_backtest_value.py       # バリュー株モード バックテスト（クロスセクション）
│
├── services/
│   ├── jquants_service.py        # J-Quants API v2 ラッパー（@st.cache_data付き）
│   ├── tdnet_service.py          # TDnet Yanoshin API クライアント
│   ├── batch_service.py          # 全銘柄メトリクス一括取得・stock_cache.csv管理
│   ├── claude_service.py         # Claude API統合（ポートフォリオ分析・IR要約）
│   ├── ir_service.py             # 適時開示フィルタリング（3層分類）・PDF抽出
│   ├── portfolio_service.py      # SBI証券CSV パーサー（CP932デコード）
│   ├── pipeline_service.py       # パイプライン本体（ハードフィルタ→スコアリング→Claude分析）
│   └── backtest_value_service.py # バリュー株バックテスト（過去スナップショット再現）
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

### バックテスト検証実績

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

#### 参考【旧数値・2026-04-29 計測】（37スナップショット × 365日 fwd、データバグ込み）
旧仕様 Top50 α+1.64% → PBR-heavy Top20 α+18.14%/勝率86.1% の比較で現行設計を決定した。
この絶対値は「ユニバース2割欠落 + 365日fwd + 生存バイアス」を含むため、現在は参照値扱い。

#### ⚠️ 期待値の現実調整
生存バイアス（廃止銘柄が prices.parquet に未収録）は依然残存。
実運用での realistic な期待 α は **+5〜10%/年**、勝率 70% 前後と見るのが妥当。

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
