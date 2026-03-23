# JV-Link Raw Data Fetcher

JRA-VAN JV-Link から蓄積系データを取得し、**生データ（SJIS/cp932）のまま**ローカルに保存するプログラム。
後工程で PostgreSQL へ投入するための中間保存を目的とする。

## 設計思想

- **パースしない** — レコードは JV-Link が返した生バイト列のまま保存
- **物理ファイル単位で保存** — JV-Link 内部のファイル境界を保持（再現性）
- **上書き更新** — 同名ファイルは最新版で上書き（過去データの訂正にも対応）
- **差分更新対応** — セットアップ完了後、前回以降の新規データのみを取得可能
- **アトミック書き込み** — `.tmp` 経由で書き込み、完了後にリネーム（電源断でもデータ破損しない）
- **dataspec ごとに逐次実行** — JV-Link の既知制約（複数 dataspec 同時指定で低速化）を回避

## 対象データ

蓄積系（`JVOpen option=4` セットアップ対象）のみ。以下は**除外**：

| 除外 | 理由 |
|------|------|
| TOKU | セットアップでは最新分のみ、過去分は option=1 が必要 |
| TCOV/RCOV/TCVN/RCVN | 今週データ系、セットアップ対象外 |
| 0Bxx (速報系) | JVRTOpen 系、セットアップ対象外 |

## 前提条件

- **Windows** （JV-Link COM は Windows 専用）
- **JV-Link インストール済み** + JRA-VAN Data Lab 会員
- **Python 3.10+**
- **pywin32**: `pip install pywin32`

## ファイル構成

```
├── main.py              # CLI エントリーポイント
├── config.py            # 定数・dataspec 定義
├── jvlink_session.py    # JV-Link COM ラッパー
├── raw_writer.py        # ファイル書き出し（アトミック書き込み）
├── job_runner.py        # ジョブ実行エンジン（セットアップ・差分取得）
├── sync_state.py        # 同期状態管理（.sync_state.json）
└── .jvconfig.json       # 保存先設定（初回実行時に自動生成）
```

## 使い方

### 1. JV-Link の設定確認

```bash
python main.py config
```

### 2. ダウンロード状態の確認

```bash
python main.py status
```

出力例:
```
=== ダウンロード済みデータ ===
dataspec  status          files  last_update
--------------------------------------------------
RACE      completed        5027  2026-02-23
DIFF      not started
DIFN      not started
BLOD      not started
...
```

### 3. セットアップ（全量ダウンロード）

```bash
# 特定の dataspec を指定
python main.py setup RACE DIFF BLOD

# 全 dataspec
python main.py setup --all

# 2020年以降のみ
python main.py setup RACE --from 20200101
```

既にセットアップ完了済みの dataspec を指定した場合、再実行の確認が行われます。
`--force` で確認をスキップできます。

### 4. 差分更新

セットアップ完了済みの dataspec に対して、前回以降の新規データのみを取得します。

```bash
# セットアップ済みの全 dataspec を更新
python main.py update

# 特定の dataspec のみ
python main.py update RACE
```

セットアップ未完了の dataspec は自動的にスキップされます。
0 からの全量ダウンロードが実行されることはありません。

### 5. データ移行（旧構造からの移行）

旧構造（`setup/RACE/timestamp/files/`）から新構造へデータを移行します。

```bash
python main.py migrate
```

### 保存先の設定

初回実行時に保存先ディレクトリを尋ねられます（デフォルト: `D:\jvdata`）。
設定は `.jvconfig.json` に保存され、次回以降は自動的に読み込まれます。

`--archive` オプションで一時的に別のパスを指定することも可能です:
```bash
python main.py status --archive E:\backup\jvdata
```

## 出力構造

```
D:\jvdata\
├── RACE\                                   # dataspec ごとのフラットなディレクトリ
│   ├── H1VM1986019920230808160612.jvd.jvdat   # 生データ（cp932）
│   ├── RAVM1986019920230808160614.jvd.jvdat
│   └── ...
├── DIFF\
│   └── ...
└── .sync_state.json                        # 全 dataspec の同期状態
```

### .sync_state.json

```json
{
  "RACE": {
    "last_timestamp": "20260223133238",
    "last_synced_at": "2026-02-28T11:26:13+00:00",
    "file_count": 5027,
    "in_progress": false
  }
}
```

- `last_timestamp`: 次回差分更新の起点（JVOpen が返す値）
- `last_synced_at`: 最後に同期完了した日時
- `file_count`: 保存済みファイル数
- `in_progress`: セットアップ中断の検出に使用

## 中断時の動作

### セットアップ中断

セットアップ中にプログラムが停止した場合、次回 `setup` 実行時に**最初からやり直し**ます。

JVOpen は毎回新しいセッションを返すため、中断後に前回スキップ済みのファイルが
JRA 側で更新されている可能性があり、安全に再開できないためです。

### 差分更新中断

差分更新中に停止した場合は、次回 `update` で同じ `last_timestamp` から再取得されます。
`last_timestamp` は全ファイル処理完了後にのみ更新されるため、データの漏れはありません。

### 書き込みの安全性

ファイル書き込みは `.tmp` 経由で行い、完了後にリネームします。
電源断や通信遮断が発生しても、元のファイルが破損することはありません。

## トラブルシューティング

### COM オブジェクト生成エラー
JV-Link が正しくインストールされているか確認：
```bash
reg query "HKCR\JVDTLab.JVLink"
```

### JVOpen が -1 や -302 を返す
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
print(type(result), result)
```
