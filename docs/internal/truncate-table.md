# TRUNCATE TABLE の設計

2026-03-10 kurosawa

## 本文書について

TRUNCATE TABLE 文の実装設計を記述する。

## 構文

```
<truncate-table>:
  TRUNCATE TABLE <table-name> [<column-identity-restart-option>]

<column-identity-restart-option>:
  CONTINUE IDENTITY
```

オプションは identity 列のリセットの有無を指定するためのものであるが、`RESTART IDENTITY` はサポートしない(理由は「サポートしないオプション」を参照)。

## 実装方針

### ストレージの再作成による全件削除

TRUNCATE TABLE は対象テーブルの全行を削除する操作であるが、jogasaki では行を個別に削除するのではなく、ストレージエントリを作り直す方式で実装する。

- `TRUNCATE TABLE T1` が実行されると、jogasaki は論理的なテーブル T1 の定義を維持したまま、shirakami に新しいストレージを作成する
  - shirakami/limestone から見ると、古いストレージとは別の新しいストレージが作成されたという扱いになる
- jogasaki はテーブル T1 の内部マッピング(`storage_manager`の`storages_`, `storage_keys_`, `storage_names_`)を新しいストレージに切り替える
  - 以降の DML はすべて新しいストレージに対して操作を行う
- `T1` がセカンダリインデックスを持つ場合は、それらのセカンダリインデックスのストレージも同様に削除予約して再作成する

### 古いストレージの遅延削除

- 古いストレージは DROP TABLE と同様の遅延削除機構（`lazy-delete-storage.md` 参照）で削除される
  - TRUNCATE文実行時点で削除予約状態とし、メンテナンススレッドへ削除処理を委譲する
    - 名前は `storage_control::name_` から削除され、`storage_control::original_name_` にコピーされる 
    - 新しいストレージ用の `storage_control` を作成し、それが同じ名前を持つ
  - これによりTRUNCATE文は古いストレージの削除(`shirakami::delete_storage`)を待たずに完了をSQLクライアントへ戻す

### IDENTITY 列の処理

- TRUNCATE TABLE はシーケンスの値・シーケンスID・シーケンス定義IDのいずれも変更しない。既存のシーケンスをそのまま引き続き使用する
  - システムテーブル(`__system_sequences`)には変更なし
  - プライマリインデックスのストレージメタデータの再作成時も、シーケンス定義IDは既存のものを使用する
- `RESTART IDENTITY` はサポートしない

### DMLとの排他制御、ロールバックの扱い

- TRUNCATE文はDDL扱いとし、通常のDDL/DML排他制御( tsurugi-issues #1230 )に従う
- DROP TABLE と同様、ロールバックはサポートしない。TRUNCATE TABLE で作成された新しいストレージや、削除予約された古いストレージは、TRUNCATE TABLEを実行したトランザクションがアボートしても取り消されない(ストレージの作成/削除予約自体がトランザクショナルでないため)。全行削除の効果はアボート後も残る

### 権限

TRUNCATE TABLE を実行するには、管理者権限またはテーブルのALTER権限が必要である。
現実装ではALTER権限が未実装のため、ALTER権限の代わりにCONTROL(ALL PRIVILEGES)権限があれば実行可能とする。

## 初期見積もり

4d

## 実装詳細

- drop_table.{h, cpp} や create_table.{h, cpp} に習って truncate_table.{h, cpp} を新規作成する。
- truncate_tableはdrop_table処理とcreate_table処理を同時に行うような実装になる
- drop_tableやcreate_table処理と内容が重複する場合は共通関数として括りだして共通化する

## テスト

下記のテストシナリオを含める

- TRUNCATE TABLE によりテーブルの全行が削除される
- 暗黙定義の主キー列があるテーブルに対して TRUNCATE TABLE を実行して全行削除を確認、その後も問題なくINSERTが可能
  - TRUNCATE TABLE前後でシステムに登録されているsequenceの個数・エントリ内容が変化しない
- generated as identity 列があるテーブルに対して TRUNCATE TABLE を実行して全行削除を確認、その後も問題なくINSERTが可能であることを確認。また、シーケンスの値がTRUNCATE前後で継続していることを確認

- DROP TABLE と同様に、lazy deleteや再起動後のリカバリのテストを実施する。
- `RESTART IDENTITY` を指定した TRUNCATE TABLE 文がコンパイルエラーになることを確認するテストを含める

## サポートしないオプション

### RESTART IDENTITY

現状では `RESTART IDENTITY` はサポートしない。指定するとコンパイルエラーになる

- TRUNCATE TABLE を実行したトランザクションがアボートした際にシーケンスに関するシステムレコードが不正な状態になることを避けるため、シーケンスID・シーケンス定義IDを維持したまま値だけをリセットする必要があった
- しかし、既存のシーケンスに新しく initial value を設定する処理は、コーナーケース(initial value が bigint の最小値かつ increment が 1 の場合など、次の値を求める減算がオーバーフローする場合)において正しく initial value をセットすることができない
- この問題を安全に解消する方法が見つからなかったため、`RESTART IDENTITY` のサポート自体を見送ることとした。

## その他

- DROP/CREATEとほぼ同じ動作になるため、TRUNCATE特有の利点は大きくないかもしれないが、テーブル/インデックス定義を維持できる点は運用前のテスト等で便利であると思われる (特にセカンダリインデックスやシーケンス列の定義がある場合)
