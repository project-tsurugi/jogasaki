# ホスト変数の取り扱い

2026-05-27 kurosawa

## 本文書について

ホスト変数の取り扱いの改善についてまとめる

## 本文書の状態

draft (subject to change) 

- 現状は block index `0` をホスト変数用に使用する方針で記述している
- variable region の設計を進め、block index を region_id として統合することで 0 番に関係なく扱う方向に変更する予定

## 背景・概要

buffer 演算子の実装に伴い、プロセスで扱う変数(以下プロセス変数とよぶ)を格納するのに複数の variable_table を扱うことが必要になった。

このために variable_table 同士を階層化(parentを保持) させる構造を導入しようとしている。(buffer-operator.md 参照)

これまではホスト変数はプロセス変数とは別の variable_table に格納され受け渡しされていたが、プロセス変数が複数の variable_table をまとめて扱う構造 (variable_container)になるのに伴い、ホスト変数も同じ仕組みの中で扱うことができるようにする。

## 目的

ホスト変数を関係演算子のコードから使用する際、プロセス変数と同じ仕組みでアクセスできるようにする。

クライアントからホスト変数の定義や値の設定を行う際のインターフェースは現状を維持する。

## 実装方針

### variable_containerのルートにホスト変数用の variable_table を設定する

buffer 演算子の実装に伴い、複数のvariable_tableをまとめて扱うvariable_containerを導入し、その variables_view を経由してアクセスすることになる。 

この variable_container に含まれる variable_table はblock indexに対応しているが、`block_index = 0` をホスト変数用に予約し、プロセスブロックは `block_index = 1` 以降を使用する（buffer-operator.md §block_index の採番ルール 参照）。

ホスト変数用 `variable_table` の `variable_container` への組み込みは `work_context` の生成時に行う。具体的には、`work_context` のファクトリが `variable_container` を構築する際、先頭要素としてホスト変数テーブルを挿入し、その後ろにプロセスブロック用テーブルを追加する。

`variable_table_info` どうしは親ポインタによって線形リストになるが、このリストのルートに対してもホスト変数を格納する `variable_table_info` を接続し、ルートがホスト変数用になるようにする。

これにより、ホスト変数はプロセス変数と同様の方法で関係演算子から使用可能になる。

例えば evaluate_bool では式評価に必要な変数を渡すのに variable_table を使っていたが、variables_view を使うようになり、その背後にある variable_container のルートにホスト変数が格納されるという構造になる。

evaluatorのコンストラクト時にホスト変数用の variable_table を渡していたが、不要になるので削除する。代わりにvariables_viewが評価時に渡される。

evaluator に配るために `process_info` にも ホスト変数用の `variable_table` を保持していたが、これも不要になるため削除する。

### ホスト変数の書き込み制約

ホスト変数テーブルは `block_index = 0` であり、すべてのプロセスブロックから見て上流に相当する。buffer-operator.md が定めるルール（下流演算子は上流ブロックの変数を書き替えることができない）により、関係演算子コードからホスト変数へ書き込むことは禁止される。

## 実装ステップ

buffer-operator.md の実装ステップと連携して行う。以下の作業は buffer-operator.md のステップ4（`variable_container` の実装）完了後に着手する。

1. `work_context` のファクトリを修正し、`variable_container` の先頭（`block_index = 0`）にホスト変数用 `variable_table` を挿入するようにする
2. ホスト変数用 `variable_table_info` を最上位プロセスブロックの `variable_table_info` の親として接続する
3. `evaluator` のコンストラクタからホスト変数用 `variable_table` 引数を削除し、`variables_view` 経由でアクセスするよう変更する
4. `process_info` からホスト変数用 `variable_table` の保持を削除する
5. 上記変更のテストを追加する
