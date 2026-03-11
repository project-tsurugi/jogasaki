# left_outer_at_most_one 結合種類の実装設計

2026-03-10 kurosawa

## 本文書について

tsurugi-issues #1432 の実装設計を記述する。`left_outer_at_most_one` という新しい結合種類に対応するため、`join` 演算子および `index_join` 演算子の両方に処理を追加する。

## 背景

- takatori の `join_kind` に `left_outer_at_most_one` が新たに定義された
- これは left outer join の一種であるが、右側の一致レコードが高々1件であることを意味する
  - 右側のレコードが 2 件以上存在する場合はエラーとして扱う

## 実装方針

### join, index_join 演算子

- `left_outer` の処理と基本的に同じだが、右側グループのレコードが 2 件以上マッチした場合にエラーを返す
  - エラーはScalarSubqueryEvaluationException (SQL-02012) を使用

## テスト方針

- SQLクエリでサブクエリを使うものを用意し、サブクエリが複数行を返すケースでのエラーを確認する
  - join, join_find, join_scan それぞれが出現するクエリを用意して試す
    - join: スカラサブクエリの結果をそのまま出力する(SELECTのリストにスカラサブクエリを含める)
    - join_find: スカラサブクエリの結果を使って主キー(全列指定)に対する検索を行う
    - join_scan: スカラサブクエリの結果を使って主キー(先頭の一部の列指定)に対する検索を行う

## 初期見積もり

2d

