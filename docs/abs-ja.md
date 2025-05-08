# ABS外部仕様

2025-04-01 nishimura

## この文書について

* この文書では `ABS` の外部仕様を示す

# 目次

1. [定義](#定義)
2. [外部仕様](#外部仕様)

## 定義

`ABS` は、数値の絶対値を取得するために **ISO/IEC 9075** で定義された関数です。
`Tsurugi` は **ISO/IEC 9075**（SQL標準）の `ABS` の仕様に準拠しています。

### 関数の構文

```
ABS(numeric_expression)
```

* `numeric_expression`：絶対値を取得する対象の数値式

`ABS` 関数は、`numeric_expression` の絶対値を返します。

## 外部仕様

* `Tsurugi` では `numeric_expression` の型として `INTEGER`、`BIGINT`、`DECIMAL`、`REAL`、`DOUBLE` のみ許容されます。
* `ABS` 関数は、入力値が負の場合は正の値に変換し、正の場合はそのままの値を返します。
* 入力値が `NULL` の場合、戻り値は `NULL` となります。
* `ABS` の結果が数値型の最大値を超える場合、エラーを発生させます。
* 入力値が `REAL` および `DOUBLE` 型の `Infinity`,`-Infinity`,`-0.0`,`NaN` のいずれかである場合は、その値をそのまま返します。
