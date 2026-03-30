# 集約処理にともなうレコード生成についてのメモ

2026-03-29 kurosawa

## 本文書について

集約処理の一部で空グループに対してレコードを生成するケースがあり、その実装について備忘のためにまとめる

## 集約処理におけるレコード生成

集約処理では、空グループに対して新しいレコードを生成する必要が生じることがある。
例えば `SELECT MAX(..) FROM T` のようなクエリでは、入力テーブルが空であっても集約関数の計算結果として1行のレコードを返す必要がある。
SQL実行エンジンはレコードベースで上流から下流へレコードが渡されていくモデルであり、基本的には1つ以上の入力レコードをもとに出力レコードが生成されるというようになっている。そのため、空の入力からレコードが生成されるというのは特殊な処理となる。

## 新規レコードを生成する可能性のある処理

新規レコードを生成する条件は下記のすべてが満たされる場合である。

  1. shuffle エクスチェンジ (つまり `group` または `aggregate` エクスチェンジ) に `group_mode::equivalence_or_whole` が設定されている
  2. grouping key 列が空である
  3. エクスチェンジの入力レコードが空である

このうち 1 と 2 は構造的な条件であり、SQLコンパイラの結果から決まる。3は実行時の条件である。
1 のエクスチェンジの種類は、使用されたのがインクリメンタル集約関数であるか(`aggregate`エクスチェンジ)、そうでないか(`group`エクスチェンジ)によって決まる。
前者の場合の処理は `aggregate` エクスチェンジで閉じているのに対し、後者は `group` エクスチェンジから `take_group` を経由して `aggregate_group` へと空グループを伝播させる必要がある。これは空グループに対する生成処理ロジックが `group` エクスチェンジではなく `aggregate_group` の知識であるためである。

下記にそれぞれのケースについて説明する

### aggregate エクスチェンジでの全体集約

aggregate エクスチェンジに `takatori::plan::group_mode::equivalence_or_whole` が設定され、かつ grouping key 列が空であるケース

例: `SELECT MAX(..) FROM empty_table` のようなクエリ。

入力レコードが存在しない場合でも、集約結果として1行を返す必要があるので下記の処理を行う

  - `group_mode::equivalence_or_whole` であってエクスチェンジへの入力が空の場合は、空グループに対する集約結果を生成して下流へ送る
  - その場合、aggregate エクスチェンジの出力が空かどうかを示すフラグ `empty_input_from_shuffle` は `false` であり、 下流 `take_group` は通常のグループ処理のパスを通る(空グループの送信処理はおこなわない)

### aggregate_group での全体集約 

group エクスチェンジで `takatori::plan::group_mode::equivalence_or_whole` が設定され、grouping key 列が空であるケース

例: `SELECT COUNT(DISTINCT ..) FROM empty_table` のようなクエリ。

- group エクスチェンジ → `take_group` → `aggregate_group` の三者が協調して `aggregate_group` へ空グループの存在を伝えて、空グループの集約処理を `aggregate_group` で行う
- group エクスチェンジが空のグループ出力となる際、`empty_input_from_shuffle` フラグを `true` にする。`take_group` がそれを確認して `aggregate_group` に通知する
  - `aggregate_group` はこの通知( `process_group(member_kind::empty)` ) を受け取った際に、空グループに対する処理を行う
  - 具体的には、空グループに対してデフォルトの集約結果を生成し、下流へ送る  

## その他 

以前は `aggregate_group` の実装は `aggregate_group::finish()` の中で新規レコードを生成して下流へ渡す実装になっていた (また group エクスチェンジの `group_mode` も確認していなかった)が、yield の仕組みと相性が悪かった（`finish()` 時の yield が無視される）。そのため実装を改善し、`take_group` が空の入力を `process_group(member_kind::empty)` として `aggregate_group` へ渡すという仕組みに変更した。

## 参考

インクリメンタル集約関数については [aggregate-exchange.md](aggregate-exchange.md) を参照
