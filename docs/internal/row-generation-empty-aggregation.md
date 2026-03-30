# 集約処理にともなうレコード生成についてのメモ

2026-03-29 kurosawa

## 本文書について

集約処理の一部で空グループに対してレコードを生成するケースがあり、その実装について備忘のためにまとめる

## 集約処理におけるレコード生成

集約処理では、空グループに対して新しいレコードを生成する必要が生じることがある。
例えば `SELECT MAX(..) FROM T` のようなクエリでは、入力テーブルが空であっても集約関数の計算結果として1行のレコードを返す必要がある。
SQL実行エンジンはレコードベースで上流から下流へレコードが渡されていくモデルであり、基本的には1つ以上の入力レコードをもとに出力レコードが生成されるというようになっている。そのため、空の入力からレコードが生成されるというのは特殊な処理となる。

## 新規レコードを生成する可能性のある処理

下記の2つの処理で、入力が空であった場合に新規レコードが生成される
2つあるのは、集約処理をエクスチェンジ内で実行するか、 `aggregate_group` 演算子内で実行するかの違いによる
関数がインクリメンタル集約関数である場合は前者、そうでない場合は後者になる。

### aggregate exchange での全体集約

aggregate exchange に `takatori::plan::group_mode::equivalence_or_whole` が設定され、かつ grouping key 列が空であるケース

例: `SELECT MAX(..) FROM empty_table` のようなクエリ。

入力レコードが存在しない場合でも、集約結果として1行を返す必要があるので下記の処理を行う

  - `group_mode::equivalence_or_whole` であって exchange への入力が空の場合は、空グループに対する集約結果を生成して下流へ送る
  - レコード生成が行われた場合はaggregateエクスチェンジの出力が空かどうかを示すフラグは `empty_input_from_shuffle=false` であり、 下流の `take_group` は通常のグループ処理のパスを通る(空グループの送信処理はおこなわない)

### aggregate_group での全体集約 

group exchange で `takatori::plan::group_mode::equivalence_or_whole` が設定され、grouping key 列が空であるケース

例: `SELECT COUNT(DISTINCT ..) FROM empty_table` のようなクエリ。

- group exchange -> take_group → aggregate_group とgroup exchange からの空グループを `take_group`/`aggregate_group` が受け取った場合、`aggregate_group` がからグループに対する集約結果を生成して下流へ送る
- group exchange が `group_mode::equivalence_or_whole` のときのみ行う。 `group_mode::equivalence` の際は空グループに対してはレコードを生成しない

- shuffle exchange が空のグループを出力する際、`take_group` が `empty_input_from_shuffle` フラグをtrueにして `aggregate_group` に通知する
  - `aggregate_group` はこのフラグを受け取った際に、空グループに対する処理を行う
  - 具体的には、空グループに対してデフォルトの集約結果を生成し、下流へ送る  

## その他 

以前は `aggregate_group` の実装は `aggregate_group::finish()` の中で新規レコードを生成して下流へ渡す実装になっていた (また `group` エクスチェンジの `group_mode` も確認していなかった)が、yield の仕組みと相性が悪かった（`finish()` 時の yield が無視される）。そのため実装を改善し、`take_group` が空の入力を `process_group(member_kind::empty)` として `aggregate_group` へ渡すという仕組みに変更した。

## 参考

インクリメンタル集約関数については [aggregate-exchange.md](aggregate-exchange.md) を参照
