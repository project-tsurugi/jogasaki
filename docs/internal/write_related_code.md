# write関連コード

## 本文書について

write処理に関連するコードがいくつかの箇所に分散しているで、その配置および関連・役割をまとめる

## write処理

- 本文書ではSQLのINSERT/UPDATE/DELETE文などによってレコードの挿入・更新・削除を行う変更処理をひろくwrite処理と呼ぶ

- write処理はtakatoriのIRでは `takatori::relation::write` および `takatori::statement::write` によって表現される
  - 前者は関係代数演算子であり、jogasakiのoperator_builderによって構築される関係代数演算子ツリーの一部をなす
    - UPDATE文、DELETE文、および INSERT into SELECT文の一部
  - 後者は `takatori::statement::statement` の子クラスであり、単独のステートメントを構成する
    - DAGや演算子ツリーを構成しないシンプルな実行単位

- takatoriの表現は2つのクラスで完結するが、jogasakiでの処理内容は豊富なためもう少し細かい単位へ分割されている

## write処理のコードの配置

write処理を表すtakatori IRとjogasakiのコードの対応を以下に示す

### `executor::common::write_statement` 

`takatori::statement::write` に対応し、(SELECTを伴わない)単独のINSERT文の実行を行うクラス

### `executor::process::impl::ops::write_existing`

`takatori::relation::write` のうち `write_kind`が`update`および`delete_`であるもの（つまりUPDATEとDELETE文）の処理を担うクラス

### `executor::process::impl::ops::write_create`

`takatori::relation::write` のうち `write_kind`が`insert`および`insert_skip`, `insert_overwrite` であるもの（つまりSELECTをともなうINSERT文）の処理を担うクラス

### `executor::wrt` 

上記の各種write関連コードに共通の処理を配置する名前空間

## indexに対するwrite処理を行うクラス

`jogasaki::index` 名前空間配下にあるクラスが具体的なインデックスへの更新操作を担当する。上記write関連コードは直接インデックスを操作せず、これらのクラスを介してインデックスを更新する。

### `index::primary_target` 

主キーに対応するインデックスへの更新操作を担うクラス

### `index::secondary_target` 

セカンダリインデックスへの更新操作を担うクラス
