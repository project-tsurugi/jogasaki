# プロセスの入出力を管理するデータ構造

2026-06-12 kurosawa

## 本文書について

プロセスがエクスチェンジを特定し関連づけを行うためのデータ構造が複数あるのでそれについて整理する

## 用語: 入出力インデックス

プロセスごとに決めたインデックスで、入力エクスチェンジ、出力エクスチェンジごとに 0 から採番される番号。

プロセス(を稼働するタスク)はこのインデックスを用いてエクスチェンジを特定し、レコードの入出力を行う。

reader index / writer index と呼ぶこともある。

## クラス: `relation_io_map`

takatoriにおける exchange と 入出力インデックスを関連付けるデータ構造

入力については `takatori::descriptor::relation` と入力インデックスのマップ。

出力については、一つの `takatori::descriptor::relation` に対する複数の出力が存在する可能性があるため、 `offer` 演算子と出力インデックスのマップにしている。

## クラス: `io_exchange_map`

入出力インデックスと jogasakiのエクスチェンジオブジェクト `executor::exchange::step` を関連付けるデータ構造

## クラス: `io_info`

各入出力インデックスに対して、エクスチェンジ上の列のメタデータ(データ型や位置など)を保持するデータ構造

## external output

プロセスの出力はエクスチェンジを経由して下流ステップに渡されるものと、`emit` 演算子を経由して外部に出力されるものがある。

後者を明示するときは external output と呼んで区別する

`emit` 演算子はステップグラフ全体で1つしか存在しないため、external output については入出力インデックスは存在せず、上記のクラスが管理するマップにも現れない

## その他・補足事項

- これら入出力インデックスを中心とするマップが複数クラスに分離しているのは幾分冗長であり、バグの温床にもなりやすいため今後整理する可能性がある

- 入出力インデックスは `compiler.cpp` 内で `relation_io_map` を作成する際に下記のように採番され、それを使って `io_exchange_map` や `io_info` が作成される。

```
executor::process::step create(
    takatori::plan::process const& process,
    compiled_info const& info,
    std::shared_ptr<mirror_container> const& mirrors,
    variable_table const* host_variables
) {
    ...
    for(std::size_t i=0, n=upstreams.size(); i < n; ++i) {
        inputs[bindings(upstreams[i])] = i;
    }
    auto offers = enumerate_offers(process);
    for(std::size_t i=0, n=offers.size(); i < n; ++i) {
        outputs[offers[i]] = i;
    }
    return {
        std::move(pinfo),
        std::make_shared<executor::process::relation_io_map>(std::move(inputs), std::move(outputs))
    };
```
