# ホスト変数の取り扱い

2026-05-27 kurosawa

## 本文書について

ホスト変数の取り扱いの改善についてまとめる

## 本文書の状態

proposed

## 背景・概要

buffer 演算子の実装に伴い、プロセスで扱う変数(以下プロセス変数とよぶ)を格納するのに複数の variable_table を扱うことが必要になった。

このために variable_table 同士を階層化(parentを保持) させる構造を導入した。(buffer-operator.md 参照)

これまではホスト変数はプロセス変数とは別の variable_table に格納され受け渡しされていたが、プロセス変数が複数の variable_table をまとめて扱う構造 (variables_view経由でのアクセス)になるのに伴い、ホスト変数も同じ仕組みの中で扱うほうが望ましい。

## 目的

ホスト変数を関係演算子のコードから使用する際、プロセス変数と同じ仕組みでアクセスできるようにする。

クライアントからホスト変数の定義や値の設定を行う際のインターフェースは現状を維持する。

## 実装方針

### variables_viewがホスト変数もアクセス可能にする

buffer 演算子の実装に伴い、複数のvariable_tableへ統一的なアクセスを提供するvariables_viewを導入した。
これは内部的には variable_table を vectorで保持し、block index で各要素にアクセスする。

ホスト変数用の variable_table もこれと同様の方法でアクセスできるようにする。ただし block index はプロセスのベーシックブロックごとに採番されたものなので region id というクラスによって抽象化してアクセスするようにする (variable-region.md 参照)。

例えば evaluate_bool では式評価に必要な変数を渡すのに variable_table を使っていたが、これを variables_view に統一する。その背後にある variable_table を隠蔽しプロセス変数用の std::vector<variable_table> やホスト変数用の variable_table は外部に露出させない。

evaluatorのコンストラクト時にもホスト変数用の variable_table を渡していたが、不要になるので削除する。代わりにvariables_viewが評価時に渡される。

evaluator に配るために `process_info` にも ホスト変数用の `variable_table` を保持していたが、これも不要になるため削除する。

ホスト変数は読み込み専用であり、演算子コードから書き込み禁止である。
