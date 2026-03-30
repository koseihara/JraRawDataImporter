# JV-Link Raw Data Downloader

JRA-VAN JV-Link から raw データを取得し、そのまま cp932 の `.jvdat` として保存する Windows 向け CLI です。

このリポジトリの責務は次に限定します。

- raw ファイルの取得
- 中断後の再開
- 保存済みアーカイブの検証
- 人間向け view の公開

このリポジトリは parser ではありません。
PostgreSQL への投入責務も持ちません。

後続の parser アプリは、この downloader が出力する raw アーカイブを読む前提です。

## 現在の正式実装

現時点の正式実装は Python です。

- package 名: `jvlink-raw-fetcher`
- CLI 名: `jvlink-raw-fetcher`
- 互換 shim: `python main.py ...`

`csharp-poc/` には比較用の .NET x86 PoC を置いていますが、本番 parity 実装ではありません。

## 必要環境

- Windows
- JV-Link を利用できる環境
- Python 3.10+
- `pywin32`

JV-Link COM を直接使うコマンドは 32bit Python が必要です。

- `setup`
- `update`
- `jvlink-config`

次のコマンドは COM を使わないので 64bit Python でも実行できます。

- `status`
- `verify`
- `refresh-view`
- `doctor`

## インストール

### 開発中のリポジトリから使う

```powershell
pip install -e .
```

### 32bit Python 環境に pywin32 を入れる

```powershell
pip install pywin32
```

## 設定ファイル

設定ファイルは user scope に保存されます。

- 既定: `%LOCALAPPDATA%\jvlink-raw-fetcher\config.json`
- 旧 `.jvconfig.json` がある場合は初回実行時に移行します

既定値:

- `archive_dir = D:\jvdata`
- `jvlink_temp_dir = C:\JVLinkTemp`

優先順位:

1. CLI 引数
2. 環境変数
3. user config
4. 既定値

環境変数:

- `JVLINK_RAW_ARCHIVE_DIR`
- `JVLINK_RAW_TEMP_DIR`
- `JVLINK_RAW_LOG_LEVEL`
- `JVLINK_RAW_CONFIG_PATH`

## コマンド

### `doctor`

利用開始前の環境診断を行います。

```powershell
jvlink-raw-fetcher doctor
jvlink-raw-fetcher doctor --archive D:\jvdata --temp-dir C:\JVLinkTemp
```

診断項目:

- Windows であるか
- 32bit Python か
- `pywin32` があるか
- JV-Link COM が登録されているか
- COM dispatch が可能か
- archive/temp dir に書けるか

### `jvlink-config`

JV-Link の設定ダイアログを開きます。

```powershell
jvlink-raw-fetcher jvlink-config
```

旧コマンド `config` も alias として残しています。

### `status`

dataspec ごとの状態を表示します。

```powershell
jvlink-raw-fetcher status
jvlink-raw-fetcher status --archive D:\jvdata
```

### `setup`

JV-Link からセットアップ取得を行います。32bit Python が必要です。

```powershell
jvlink-raw-fetcher setup RACE
jvlink-raw-fetcher setup RACE DIFF BLOD
jvlink-raw-fetcher setup --all
jvlink-raw-fetcher setup RACE --from 20200101
```

### `update`

公開済み `current` の `last_successful_timestamp` から差分更新します。32bit Python が必要です。

```powershell
jvlink-raw-fetcher update
jvlink-raw-fetcher update RACE
```

### `verify`

アーカイブの整合性検査を行います。

```powershell
jvlink-raw-fetcher verify RACE --archive D:\jvdata
jvlink-raw-fetcher verify --all --archive D:\jvdata
```

### `refresh-view`

人間向けの `view/current` と `view/previous` を再生成します。

```powershell
jvlink-raw-fetcher refresh-view RACE --archive D:\jvdata
jvlink-raw-fetcher refresh-view --all --archive D:\jvdata
```

## ディレクトリ構成

```text
D:\jvdata\
  RACE\
    refs\
      current.json
      previous.json
    commits\
      <commit_id>\
        meta.json
        manifest.jsonl
    objects\
      ab\
        abcdef....jvdat
    runs\
      <run_id>\
        run_state.json
        staging\
        candidate_manifest.jsonl
    view\
      current\
        H1\
        RA\
        SE\
      previous\
        H1\
        RA\
        SE\
```

## 後続アプリに渡す公開面

後続の parser が依存してよい公開面は次の 3 つです。

- `view/current/<format_code>/<logical_filename>.jvdat`
- `refs/current.json`
- `commits/<commit_id>/manifest.jsonl`

`objects/` は内部実装です。

## 安全性

- 取得中のファイルは `runs/<run_id>/staging` にだけ書きます
- 成功時だけ `refs/current.json` を更新します
- 途中で失敗した run は `runs/<run_id>` に残り、再開できます
- partial file は完了扱いにしません
- ひとつ前の公開状態は `refs/previous.json` に残します

## 推奨運用

1. `doctor` で環境診断を通す
2. `jvlink-config` で JV-Link の設定を確認する
3. `setup RACE` で初回取得する
4. `verify RACE` で整合性を確認する
5. `update RACE` を継続実行する
6. 必要に応じて `refresh-view RACE` を実行する

## ドキュメント

- [現状アーキテクチャ整理](docs/current-architecture.md)
- [Python と C# の比較メモ](docs/python-vs-csharp-comparison.md)

## トラブルシュート

### `JV-Link COM is expected to run under 32-bit Python`

`setup` / `update` / `jvlink-config` を 64bit Python で実行しています。32bit Python で再実行してください。

### `JVOpen failed: -302`

JV-Link の利用条件や設定が満たされていない可能性があります。`jvlink-config` を開いて設定を確認してください。

### `verify` が失敗する

object の欠損や破損が起きています。`current` と `previous` のどちらが壊れているかを確認し、必要なら再取得を検討してください。
