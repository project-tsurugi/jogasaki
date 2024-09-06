# SQLデータ型の実装

2023-03-24 kurosawa

## この文書について

SQL実行エンジンのデータ型の実装における内部仕様について記述する

外部へ公開している仕様については [value_limit.md](https://github.com/project-tsurugi/jogasaki/blob/master/docs/value_limit.md) を参照

## 仕様

## `TINYINT`/`SMALLINT`/`INT`/`BIGINT`

- 実行時型: `std::int32_t` (`TINYINT`, `SMALLINT`, `INT`) / `std::int64_t` (`BIGINT`)
  - 全順序で比較可能

## `REAL`/`DOUBLE`

- 実行時型: float (`REAL`) / double (`DOUBLE`)
  - `NaN` は他のすべての値(`+Infinity`含む)より大きな値として扱い、自分自身と等しいものとして比較する (IEEE 754規則と異なる)
  - 計算過程/結果に現れた`NaN`は最小の`quiet NaN`に正規化する。正規でない`NaN`が外部へ見えることはない。
  - `+0.0`と`-0.0`は互いに等しいものとして比較する
    - このルールのため全順序ではない(strict weak ordering)
  - `-0.0`が計算過程/結果に現れることはある
  - `-0.0`はストア時には`+0.0`へ正規化する

## `VARCHAR`

- 実行時型: `jogasaki::accessor::text`
  - 全順序で比較可能
  - UTF8文字列を想定

### `VARCHAR(n)`
  - 長さ `n` バイト以下の文字列を格納できる

### `VARCHAR(*)`

- `VARCHAR`には任意長 `*` を指定することができる
  - キャスト式の場合、長さに制限のない文字列型
  - 列定義の場合、既定の最大長で置き換えられる(ストレージサイズの制約)

### `VARCHAR` (カッコなし)

- カッコなしの `VARCHAR` は `VARCHAR(*)` と同じ意味
  - 列定義とキャストで共通

## `CHAR`

- 実行時型: `jogasaki::accessor::text`
  - 全順序で比較可能
  - UTF8文字列を想定

### `CHAR(n)`
  - 長さ `n` バイトに満たない場合は空白文字(U+0020)によってパディングされる

### `CHAR(*)`

- `CHAR`に任意長 `*` を指定することはできない
  - パディング長をきめられないため
  - 列定義とキャストで共通

### `CHAR` (カッコなし)

- カッコなしの`CHAR`を指定すると `CHAR(1)`の意味となる
  - 列定義とキャストで共通

## `BINARY`

- 実行時型: `jogasaki::accessor::binary`
  - 全順序で比較可能
- `BINARY(n)`, `BINARY(*)`, `BINARY` (カッコなし) の長さに関する扱いは `CHAR` の各ケースと同様
  - ただしパディング文字はヌル(U+0000)を使用する

## `VARBINARY`

- 実行時型: `jogasaki::accessor::binary`
  - 全順序で比較可能
  - ただしエンコーディング上の制限により主キーおよびインデックス列には使用できない
- `VARBINARY(n)`, `VARBINARY(*)`, `VARBINARY` (カッコなし) の長さに関する扱いは `VARCHAR` の各ケースと同様

## DECIMAL

- 桁数が限定された十進数を扱う型
- 型パラメーターとして`p`(precision), `s`(scale)を持つ
  - `p` は `[1,38]` の範囲の整数で桁数を表す
  - `s`は `[0, p]` の範囲の整数で小数点以下の桁数を表す
- `DECIMAL(p,s)`は`[-(10^p-1)*10^(-s), (10^p-1)*10^(-s)]` の範囲の値を表現可能
- 実行時型: `takatori::decimal::triple`
  - `triple` そのものには順序比較がないので `decimal::Decimal` へ変換して順序比較をおこなう
    - strict weak order
      - 下記のように互いに等しく比較される複数の `triple` がある
  - 任意の `triple` は `decimal::Decimal` へ変換可能 
    - ただしこの変換は単射ではない
      - trailing zeroを加えて指数を調整した `triple` も同じ `Decimal` に変換される
    - trailing zeroを除けば一意なので、演算や変換は可能な限りtrailing zeroを除去した形(reduced)で出力するようにする

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
