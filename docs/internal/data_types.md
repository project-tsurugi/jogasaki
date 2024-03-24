# SQLデータ型の実装

2023-03-24 kurosawa

## この文書について

SQL実行エンジンのデータ型の実装における内部仕様について記述する

外部へ公開している仕様については [value_limit.md](https://github.com/project-tsurugi/jogasaki/blob/master/docs/value_limit.md) を参照

## 仕様

## TINYINT/SMALLINT/INT/BIGINT

- 実行時型: std::int32_t (TINYINT, SMALLINT, INT) / std::int64_t (BIGINT)
- 実行時型の値の一意性：あり
- 全順序性：あり

## REAL/DOUBLE

- 実行時型: float (REAL) / double (DOUBLE)
- 実行時型の値の一意性：なし
  - -0.0 があるので一意ではない
  - ストア時には -0.0は正のゼロへ正規化する
- 全順序性：あり
  - NaNを最大の値として扱いNaN == NaNとする
  - 計算結果で現れたNaNは最小のquiet NaNにnormalizeする

## VARCHAR

- 実行時型: jogasaki::accessor::text
- 実行時型の値の一意性：あり
- 全順序性：あり

### `VARCHAR(*)`

- `VARCHAR`には任意長 `*` を指定することができる
  - キャスト式の場合、長さに制限のない文字列型
  - 列定義の場合、既定の最大長で置き換えられる(ストレージサイズの制約)

### `VARCHAR` (カッコなし)

- カッコなしの`VARCHAR`を指定するとコンパイルエラー
  - 列定義とキャストで共通

## CHAR

- 実行時型: jogasaki::accessor::text
- 実行時型の値の一意性：あり
- 全順序性：あり

### `CHAR(*)`

- `CHAR`に任意長 `*` を指定することはできない
  - パディング長をきめられないため
  - 列定義とキャストで共通

### `CHAR` (カッコなし)

- カッコなしの`CHAR`を指定すると `CHAR(1)`の意味となる
  - 列定義とキャストで共通

## DECIMAL

- 桁数が限定された十進数を扱う型
- 型パラメーターとして`p`(precision), `s`(scale)を持つ
  - `p` は `[1,38]` の範囲の整数で桁数を表す
  - `s`は `[0, p]` の範囲の整数で小数点以下の桁数を表す
- `DECIMAL(p,s)`は`[-(10^p-1)*10^(-s), (10^p-1)*10^(-s)]` の範囲の値を表現可能
- 実行時型: takatori::decimal::triple
- 実行時型の値の一意性：あり
  - trailing zeroを除去した形のtripleのみをあつかう
- 全順序性：あり

### 任意長のprecision/scale

- precisionとscaleに任意長 `*` を指定することも可能
  - precision/scale両方が `*` のとき
    - 実装が既定する可能な限り広い範囲の型
      - `takatori::decimal::triple` を適切に限定する
      - 係数の桁数は最大38まで
      - 調整後指数(adjusted exponent)が `[-24575, 24576]` の範囲
    - 表定義における列の型としては使用不可、キャスト先の型としては可能
  - precisionが `*` でscaleが `*` でない場合
    - precisionは最大の38となる
      - `DECIMAL(*, s)` == `DECIMAL(38, s)`
  - precisionが `*` でなくscaleが `*` の場合
    - エラーになる(浮動小数点数に対して桁数を固定することは意味がない)
- 「`DECIMAL(*,*)` がキャスト式のみで使用可能」という点以外は列定義とキャストで共通

### `DECIMAL(p)`
- precisionのみ指定されscaleが省略された場合は `0` が指定されたものとみなす
  - `DECIMAL(p)` == `DECIMAL(p, 0)`

### `DECIMAL` (カッコなし)

- カッコが省略された場合は既定の最大桁の整数となる
  - `DECIMAL` == `DECIMAL(38, 0)`
