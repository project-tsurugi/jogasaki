# TRUNCATE TABLE の設計

2026-03-10 kurosawa

## 本文書について

TRUNCATE TABLE 文の実装設計を記述する。

## 構文

```
<truncate-table>:
  TRUNCATE TABLE <table-name> [<column-identity-restart-option>]

<column-identity-restart-option>:
  RESTART IDENTITY
  CONTINUE IDENTITY
```

オプションは identity 列のリセットの有無のみである。

## 実装方針

### ストレージの再作成による全件削除

TRUNCATE TABLE は対象テーブルの全行を削除する操作であるが、jogasaki では行を個別に削除するのではなく、ストレージエントリを作り直す方式で実装する。

- `TRUNCATE T1` が実行されると、jogasaki は論理的なテーブル T1 の定義を維持したまま、shirakami に新しいストレージを作成する
  - shirakami/limestone から見ると、古いストレージとは別の新しいストレージが作成されたという扱いになる
- jogasaki はテーブル T1 の内部マッピングを新しいストレージに切り替える
  - 以降の DML はすべて新しいストレージに対して操作を行う

### 古いストレージの遅延削除

- 古いストレージは DROP TABLE と同様の遅延削除機構（`lazy-delete-storage.md` 参照）で削除される
  - TRUNCATE 実行時点で参照カウンタが 0 であれば `delete_storage` を実行するタスクをスケジュールする
  - 参照カウンタが 0 でなければ削除予約状態とし、カウンタが 0 になった時点で同様のタスクをスケジュールする
  - いずれにしても、TRUNCATE文は古いストレージの削除完了を待たずに完了する 

### IDENTITY 列の処理

- `RESTART IDENTITY` が指定された場合、TABLE に紐づくすべての identity 列のシーケンスを初期値にリセットする
- `CONTINUE IDENTITY`（デフォルト）が指定された場合、identity 列のシーケンスはリセットしない

### DMLとの排他制御 

- TRUNCATE文はDDL扱いとし、通常のDDL/DML排他制御( tsurugi-issues #1230 )に従う

## 初期見積もり

4d

## その他・要調査事項

- DROP/CREATEとほぼ同じ動作になるため、TRUNCATE特有の利点は大きくないかもしれない？
- delete_storage 完了前にクラッシュ等で古いストレージが残った場合の対処
- セカンダリインデックスを持つテーブルの TRUNCATE については、すべてのインデックスストレージを同様に再作成する必要がある
