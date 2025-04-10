# MOD外部仕様

2025-04-10 nishimura

## この文書について

* この文書では `MOD` と`%`の外部仕様を示す

# 目次

1. [定義](#定義)
2. [外部仕様](#外部仕様)

## 定義

`MOD` は、剰余（余り）を計算するために **ISO/IEC 9075**（SQL標準）で定義された関数です。
`Tsurugi` は **ISO/IEC 9075** の `MOD` の仕様に準拠しています。
`Tsurugi` では`MOD`と`%`は同じ動作をします。

### 関数の構文

```
MOD(numeric_expression_1, numeric_expression_2)
```

* `numeric_expression_1`：被除数（剰余を求める対象の数値式）
* `numeric_expression_2`：除数（0 以外の数値式）

`MOD` 関数は、`numeric_expression_1` を `numeric_expression_2` で除算したときの剰余を返します。
剰余の符号は **被除数（第1引数）と同じ** になります（ISO SQL 準拠）。

## 外部仕様

* `Tsurugi` では `numeric_expression_1` および `numeric_expression_2` の型として `INTEGER`、`BIGINT`、`DECIMAL`のみ許容されます。**ただし将来的に変更になる可能性がある。**
* 除数（第2引数）がゼロの場合、エラーが発生します。
* いずれかの引数が `NULL` の場合、戻り値は `NULL` になります。
* 被除数と除数の型が異なる場合、精度がより高い型へ自動的に昇格します。結果の値は被除数及び除数と同じ型になります。