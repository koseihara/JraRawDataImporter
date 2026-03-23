# JV-Link Raw Data Fetcher

JRA-VAN JV-Link から蓄積系データを取得し、**生データ（SJIS/cp932）のまま**ローカルに保存するプログラム。  
後工程で PostgreSQL へ投入するための中間保存を目的とする。

## 設計思想

- **パースしない** — レコードは JV-Link が返した生バイト列のまま保存
- **物理ファイル単位で保存** — JV-Link 内部のファイル境界を保持（再現性）
- **中断・再開対応** — `job_state.json` でファイル単位の進捗を管理
- **dataspec ごとに逐次実行** — JV-Link の既知制約（複数 dataspec 同時指定で低速化）を回避

## 対象データ

蓄積系（`JVOpen option=4` セットアップ対象）のみ。以下は**除外**：

| 除外 | 理由 |
|------|------|
| TOKU | セットアップでは最新分のみ、過去分は option=1 が必要（今回不要） |
| TCOV/RCOV/TCVN/RCVN | 今週データ系、セットアップ対象外 |
| 0Bxx (速報系) | JVRTOpen 系、セットアップ対象外 |

## 前提条件

- **Windows** （JV-Link COM は Windows 専用）
- **JV-Link インストール済み** + JRA-VAN Data Lab 会員
- **Python 3.10+**
- **pywin32**: `pip install pywin32`

## ファイル構成

```
jvlink_fetcher/
├── main.py              # CLI エントリーポイント
├── config.py            # 定数・dataspec 定義
├── jvlink_session.py    # JV-Link COM ラッパー
├── raw_writer.py        # ファイル書き出し + manifest
├── job_runner.py        # ジョブ実行エンジン
└── job_state.py         # 中断再開用の状態管理
```

## 使い方

### 1. JV-Link の設定確認

```bash
python main.py config
```

### 2. 全蓄積系データのセットアップ取得

```bash
# 1986年から全データ（デフォルト）
python main.py setup --archive D:\jvdata

# 2020年以降のみ
python main.py setup --archive D:\jvdata --from 20200101

# 特定の dataspec のみ
python main.py setup --archive D:\jvdata --dataspecs RACE DIFF BLOD
```

### 3. 単一 dataspec の実行（テスト・再開用）

```bash
python main.py single --archive D:\jvdata RACE
```

### 4. 進捗確認

```bash
python main.py status --archive D:\jvdata
```

## 出力構造

```
D:\jvdata\
└── setup\
    ├── RACE\
    │   └── 20260226_101530\          # ジョブ実行タイムスタンプ
    │       ├── files\
    │       │   ├── RACE_001.jvdat    # 生データ（cp932）
    │       │   ├── RACE_002.jvdat
    │       │   └── ...
    │       ├── manifest.jsonl         # ファイルごとのメタデータ
    │       └── job_state.json         # 中断再開用の状態
    ├── DIFF\
    │   └── ...
    └── ...
```

### manifest.jsonl の各行

```json
{
  "jvlink_filename": "RACE20260101.jvd",
  "output_file": "RACE20260101.jvd.jvdat",
  "record_count": 12345,
  "byte_count": 6789012,
  "sha256": "a1b2c3d4...",
  "started_at": "2026-02-26T10:15:30+00:00",
  "completed_at": "2026-02-26T10:16:45+00:00"
}
```

### job_state.json

```json
{
  "dataspec": "RACE",
  "mode": "setup",
  "option": 4,
  "fromtime": "19860101000000",
  "status": "completed",
  "last_processed_filename": "RACE20260101.jvd",
  "processed_files": 42,
  "processed_records": 1234567,
  "processed_bytes": 567890123,
  "attempt_count": 1
}
```

## 中断・再開

プログラムが途中で停止した場合、同じコマンドを再実行すれば自動的に再開される：

1. `job_state.json` から `last_processed_filename` を読む
2. `JVOpen` を再実行（同じパラメータ）
3. `last_processed_filename` までレコードをスキップ
4. 次のファイルから書き込みを再開

## トラブルシューティング

### COM オブジェクト生成エラー
JV-Link が正しくインストールされているか確認：
```bash
# レジストリ確認
reg query "HKCR\JVDTLab.JVLink"
```

### JVOpen が -1 を返す
- dataspec の綴りを確認
- JRA-VAN の会員資格・ログイン状態を確認
- `python main.py config` で設定ダイアログを開いて認証

### COM の戻り値が期待と異なる
`jvlink_session.py` の `open()` / `read()` メソッドで、
COM の [out] パラメータの返し方を自動判定しているが、
環境によってはタプルの構造が異なる場合がある。

デバッグ方法:
```python
import win32com.client
jv = win32com.client.Dispatch("JVDTLab.JVLink")
jv.JVInit("")
result = jv.JVOpen("RACE", "20260101000000", 4, 0, 0, "")
print(type(result), result)  # ← これで構造を確認
```

## 次のステップ

1. **Windows 環境でテスト実行** — まず `RACE` 単体で動作確認
2. **COM 戻り値の確認** — 上記デバッグ方法で構造を把握
3. **全 dataspec セットアップ** — 問題なければ全データ取得
4. **パーサー開発** — JV-Data 仕様書の固定長レコードをパース
5. **PostgreSQL 投入** — スキーマ設計 → バルクインサート
