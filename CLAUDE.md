# stock_analysis — プロジェクト概要

> **セッション開始時の必須手順**: CLIのメモリが `C:\Users\ryohei\.claude\projects\C--Users-ryohei\memory\` にあります。`MEMORY.md` を読み込んでから各メモリファイルを参照してください。



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
│   └── 5_portfolio_analysis.py   # AI分析（Claude APIによる総評・銘柄別売買提案）
│
├── services/
│   ├── jquants_service.py        # J-Quants API v2 ラッパー（@st.cache_data付き）
│   ├── tdnet_service.py          # TDnet Yanoshin API クライアント
│   ├── batch_service.py          # 全銘柄メトリクス一括取得・stock_cache.csv管理
│   ├── claude_service.py         # Claude API統合（ポートフォリオ分析・IR要約）
│   ├── ir_service.py             # 適時開示フィルタリング（3層分類）・PDF抽出
│   └── portfolio_service.py      # SBI証券CSV パーサー（CP932デコード）
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
│   ├── backtest_records.csv      # バックテスト詳細トレード記録
│   └── backtest_summary.csv      # バックテスト集計統計
│
└── styles/
    └── custom.css                # カスタムCSS（スマホ対応含む）
```

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
- 権利修正済み: `AdjO/H/L/C/Vo` を使用

### fins/summary 主要カラム
- 実績: `Sales, OP, NP, EPS, BPS, Eq, TA, CFO`（単位: **百万円**。EPS/BPS は円/株）
- 配当: `DivAnn, FDivAnn`（予想年間配当）
- 予想: `FSales, FOP, FNP, FEPS`（今期予想）
- `CurPerType` — `FY`（通期） / `1Q`〜`3Q`（四半期）
- `DiscDate` — 開示日

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
