# stock_analysis — プロジェクト概要

日本株のスクリーニング・分析ツール。J-Quants API v2 を主データソースとし、TDnet MCP で適時開示情報を補完する。

---

## 環境

| 項目 | 内容 |
|------|------|
| Python | 3.9.1 |
| 仮想環境 | `.venv/` （常に `.venv/Scripts/python.exe` を使用） |
| 依存パッケージ | `requirements.txt` 参照 |
| 認証情報 | `.env`（Git管理外） |

### .env キー
```
JQUANTS_API_KEY=...   # J-Quants v2 APIキー（x-api-key ヘッダー認証）
JQUANTS_MAIL=...      # J-Quants ログインメール（v1 token取得用。通常不要）
JQUANTS_PASS=...      # J-Quants ログインパスワード（同上）
```

---

## データソース

### 1. J-Quants API v2（メイン）
- **プラン**: 有料プラン（Premium）
- **認証**: `x-api-key` ヘッダーに `JQUANTS_API_KEY` をセット
- **Base URL**: `https://api.jquants.com/v2`

#### 主要エンドポイント

| エンドポイント | 説明 | 主なパラメータ |
|--------------|------|--------------|
| `GET /equities/master` | 上場銘柄マスタ（4443件） | なし |
| `GET /equities/bars/daily` | 日次OHLCV（権利修正済含む） | `code`, `from`, `to` |
| `GET /fins/summary` | 財務サマリー（決算短信ベース） | `code` |

#### equities/master 主要カラム
- `Code` — 銘柄コード（5桁）
- `CoName` — 会社名（日本語）
- `CoNameEn` — 会社名（英語）
- `Mkt` / `MktNm` — 市場コード / 市場名
- `S17Nm` / `S33Nm` — セクター分類

#### 市場コード（Mkt）
| コード | 市場 |
|--------|------|
| `0111` | 東証プライム |
| `0112` | 東証スタンダード |
| `0113` | 東証グロース |

#### equities/bars/daily 主要カラム
`Date, Code, O, H, L, C, Vo, Va, AdjFactor, AdjO, AdjH, AdjL, AdjC, AdjVo`
- 権利修正済み: `AdjO/H/L/C/Vo` を使用

#### fins/summary 主要カラム
- 実績: `Sales, OP, NP, EPS, BPS, Eq, TA, CFO`
- 配当: `DivAnn, FDivAnn`（予想年間配当）
- 予想: `FSales, FOP, FNP, FEPS`（今期予想）
- `ShOutFY` — 期末発行済株式数
- `CurPerType` — `FY`（通期） / `1Q`〜`3Q`（四半期）
- `DiscDate` — 開示日

### 2. TDnet MCP（適時開示）
Claude Code 環境に MCP サーバーとして設定済み。以下のツールが利用可能：

| ツール | 説明 |
|--------|------|
| `get_latest_disclosures` | 最新の適時開示一覧（最大300件） |
| `search_disclosures` | キーワード検索（**会社名で検索**。銘柄コード `7203` では0件になる） |
| `get_company_disclosures` | 特定銘柄の開示一覧（4桁コード指定） |
| `get_disclosures_by_date` | 日付指定で取得（1日分のデータ量が多いため **limit を必ず指定**すること） |

---

## ファイル構成

```
stock_analysis/
├── .env                        # 認証情報（Git管理外）
├── .gitignore
├── .venv/                      # Python仮想環境（3.9.1）
├── requirements.txt
├── screener.py                 # スクリーナーコアロジック
├── test_login.py               # J-Quants 接続テスト
├── screening_result_YYYYMMDD.csv  # スクリーニング結果出力
│
├── app.py                      # Streamlit エントリーポイント
├── pages/
│   ├── 1_screening.py          # スクリーニングページ
│   ├── 2_stock_detail.py       # 銘柄詳細ページ（チャート・財務・開示）
│   └── 3_disclosures.py        # 適時開示ブラウザ
├── services/
│   ├── jquants_service.py      # J-Quants API v2 キャッシュ付きラッパー
│   └── tdnet_service.py        # TDnet Yanoshin API クライアント
├── components/
│   ├── chart.py                # Plotly OHLCVチャート生成
│   ├── financial_cards.py      # 財務指標カード・スコアバッジ
│   └── disclosure_table.py     # 適時開示テーブル
└── styles/
    └── custom.css              # カスタムCSS
```

## Streamlit アプリ起動

```bash
cd C:\Users\ryohei\stock_analysis
.venv\Scripts\streamlit.exe run app.py
# → http://localhost:8501 でブラウザアクセス
```

## アプリ構成（3ページ）

| ページ | 説明 |
|--------|------|
| スクリーニング | サイドバーで条件設定 → J-Quants で銘柄抽出 → 結果テーブル |
| 銘柄詳細 | Plotly インタラクティブチャート・財務指標・TDnet開示（タブ構成） |
| 適時開示 | TDnet Yanoshin API で最新開示/日付指定/銘柄検索 |

## TDnet Yanoshin API（アプリ用）

- URL: `https://webapi.yanoshin.jp/webapi/tdnet/list/{YYYYMMDD}.json`
- パラメータ: `limit`, `company_code`（4桁）
- レスポンス: `{"total_count": N, "items": [{"Tdnet": {id, pubdate, company_code, company_name, title, document_url, markets_string}}]}`
- ※ TDnet MCP は Claude Code 専用。Streamlit アプリからは直接 HTTP で上記 API を使用

---

## screener.py の構造

### クラス

**`JQuantsClient`**
- `x-api-key` ヘッダー認証で J-Quants v2 に接続
- `get_listed_info()` — 銘柄マスタ取得
- `get_daily_quotes(code, from_date, to_date)` — 日次株価取得（AdjC/AdjVo使用）
- `get_financials(code)` — 財務サマリー取得（FYのみフィルタ）

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

**`StockScreener`**
- `run(market_codes, max_stocks, delay_sec, to_date, from_date)` — スクリーニング実行
- スコア算式: PER(25点) + PBR(15点) + ROE(20点) + 売上成長(20点) + RSI(10点)

### スコア算式
```python
score += max(0, (20 - per) / 20 * 25)    # PER: 低いほど高得点
score += max(0, (1.5 - pbr) / 1.5 * 15)  # PBR: 低いほど高得点
score += min(roe / 20 * 20, 20)           # ROE: 高いほど高得点（上限20点）
score += min(rev_growth / 20 * 20, 20)    # 売上成長: 高いほど高得点（上限20点）
score += max(0, 10 - abs(rsi - 50) / 5)  # RSI: 50に近いほど高得点
```

---

## 実行方法

```bash
cd C:\Users\ryohei\stock_analysis

# スクリーナー実行（東証プライム、先頭500銘柄）
.venv/Scripts/python.exe screener.py

# スクリプト内での load_dotenv は引数なしでOK（__main__実行時）
# -c フラグで実行する場合は load_dotenv('.env') と明示する
```

---

## 注意事項

- `.env` は `.gitignore` で除外済み。APIキーをコードにハードコードしない。
- `load_dotenv()` は `-c` フラグ実行時に `AssertionError` になるため、`load_dotenv('.env')` と明示する。
- J-Quants の銘柄コードは **5桁**（例: `72030` = トヨタ）。東証4桁コードの末尾に `0` を付与。
- Rate limit (429) 時は60秒待機してリトライ（`JQuantsClient._get` に実装済み）。
- `fins/summary` の財務数値単位は **百万円**（`Sales`, `NP`, `Eq` 等）。`EPS`, `BPS` は **円/株**。
