# ROUND外部仕様

2025-06-09 nishimura

## この文書について

* この文書では `ROUND` の外部仕様を示す

# 目次

1. [定義](#定義)
2. [外部仕様](#外部仕様)

## 定義

`ROUND` は、与えられた数値を指定された桁数で四捨五入し、丸められた値を返す関数です。

### 関数の構文

```
ROUND(numeric_expression[, integer_expression])
```

* `numeric_expression`：丸めの対象となる数値式
* `integer_expression`（省略可能）：

   * `integer_expression` が **0 以上** の場合：  
     小数点以下の桁数を指定し、**指定した桁数で四捨五入**します。  
     - `1` → 小数第1位まで残す（例：`123.456 → 123.5`）  
     - `0` → 小数点以下を考慮して **整数に四捨五入**（例：`123.456 → 123`）  

   * `integer_expression` が **負の値** の場合：  
     **整数部のうち** `10^(-integer_expression)` **の位で四捨五入**します。  
     - `-1` → 10の位で四捨五入（例：`123.456 → 120`）  
     - `-2` → 100の位で四捨五入（例：`123.456 → 100`） 

`ROUND` 関数は、`numeric_expression` を `integer_expression` で指定された桁数に四捨五入した結果を返します。`integer_expression` を省略した場合は、小数点以下0桁（整数）に丸められます。

## 外部仕様

* `Tsurugi` では `numeric_expression` の型として `INTEGER`、`BIGINT`、`DECIMAL`、`REAL`、`DOUBLE` のみ許容されます。
* `integer_expression` の型としては `INTEGER` または `BIGINT` を許容します。
* `numeric_expression` または `integer_expression` のいずれかが `NULL` の場合、戻り値は `NULL` となります。
* `numeric_expression` が `REAL` または `DOUBLE` 型の `Infinity`、`-Infinity`、`-0.0`、`NaN` のいずれかである場合は、その値をそのまま返します。
* `integer_expression` にはnumeric_expressionの型に対応した上下限値が定められており、その範囲を超えた場合エラーとなります。

| `numeric_expression` の型 | 上限値 | 下限値 |
| -------------------------| ----- | ----- |
| `INT`                    | +0    | -9    |
| `BIGINT`                 | +0    | -18   |
| `REAL`                   | +7    | -7    |
| `DOUBLE`                 | +15   | -15   |
| `DECIMAL`                | +38   | -38   |


