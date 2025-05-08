# FLOOR外部仕様

2025-05-08 nishimura

## この文書について

* この文書では `FLOOR` の外部仕様を示す

# 目次

1. [定義](#定義)
2. [外部仕様](#外部仕様)

## 定義

`FLOOR` は、与えられた数値を切り下げて、最も近い整数値を返すために **ISO/IEC 9075** で定義された関数です。
`Tsurugi` は **ISO/IEC 9075**（SQL標準）の `FLOOR` の仕様に準拠しています。

### 関数の構文

```
FLOOR(numeric_expression)
```

* `numeric_expression`：切り下げの対象となる数値式

`FLOOR` 関数は、`numeric_expression` の以下の最小の整数値を返します。

## 外部仕様

* `Tsurugi` では `numeric_expression` の型として `INTEGER`、`BIGINT`、`DECIMAL`、`REAL`、`DOUBLE` のみ許容されます。
* 入力値が `NULL` の場合、戻り値は `NULL` となります。
* 入力値が `REAL` および `DOUBLE` 型の `Infinity`,`-Infinity`,`-0.0`,`NaN` のいずれかである場合は、その値をそのまま返します。
