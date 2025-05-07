# CEIL外部仕様

2025-05-08 nishimura

## この文書について

* この文書では `CEIL` の外部仕様を示す

# 目次

1. [定義](#定義)
2. [外部仕様](#外部仕様)

## 定義

`CEIL` は、与えられた数値を切り上げて、最も近い整数値を返すために **ISO/IEC 9075** で定義された関数です。
`Tsurugi` は **ISO/IEC 9075**（SQL標準）の `CEIL` の仕様に準拠しています。

### 関数の構文

```
CEIL(numeric_expression)
```

* `numeric_expression`：切り上げの対象となる数値式

`CEIL` 関数は、`numeric_expression` の以上の最小の整数値を返します。

## 外部仕様

* `Tsurugi` では `numeric_expression` の型として `INTEGER`、`BIGINT`、`DECIMAL`、`FLOAT`、`DOUBLE` のみ許容されます。
* 入力値が `NULL` の場合、戻り値は `NULL` となります。
