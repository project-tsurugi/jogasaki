# SQL DECIMAL

2023-12-25 kurosawa

## 本文書について

TsurugiにおけるDECIMAL型についての仕様詳細と実装上のメモ

## DECIMALとmpdecimal

- jogasakiとtakatoriはSQLの`DECIMAL`型の実装に[mpdecimal](https://www.bytereef.org/mpdecimal/)を使用する
- mpdecimalはSQLの`DECIMAL(p,s)`よりも広い範囲の数値を扱う事が可能であり、計算過程の値などは`DECIMAL(p,s)`よりもmpdecimalの型の方が便利に取り扱うことができる
  - 例えばDECIMALを使用した場合`DECIMAL(p,s)`と`DECIMAL(p', s')`の乗算は `DECIMAL(p+p', s+s')`型となり演算によって型で閉じないことがありえるが、mpdecimalの型を用いるとこれを避けられる
- mpdecimalのような実装によって実現される「実装が許容する可能な限り広い範囲のDECIMAL」の型を`DECIMAL(*,*)`と表記する
  - 外部から見える範囲では `takatori::decimal::triple` (の桁数と指数を制限したもの)
  - レコード(record_ref)に格納される列データやクエリ結果はこの型(相当のもの)が使用される
  - jogasakiがDECIMALの数値演算を行う際は、更に広い範囲を扱うことが可能な `decimal::Decimal` を用いて行われるが外部へ見えることはない

## TsurugiのDECIMALの仕様

- 桁数が限定された十進数を扱う型
- 型パラメーターとして`p`(precision), `s`(scale)を持つ
  - `p` は `[1,38]` の範囲の整数で桁数を表す
  - `s`は `[0, p]` の範囲の整数で小数点以下の桁数を表す
- `DECIMAL(p,s)`は`[-(10^p-1)*10^(-s), (10^p-1)*10^(-s)]` の範囲の値を表現可能

- precisionとscaleに任意長 `*` を指定することも可能
  - precision/scale両方が `*` のとき
    - 実装が既定する可能な限り広い範囲の型
      - 実装上は `takatori::decimal::triple` で表現される
      - 係数の桁数は最大38まで
      - 調整後指数(adjusted exponent)が `[-24575, 24576]` の範囲
    - 表定義における列の型としては使用不可、キャスト先の型としては可能
  - precisionが `*` でscaleが `*` でない場合
    - precisionは最大の38となる
      - `DECIMAL(*, s)` == `DECIMAL(38, s)`
  - precisionが `*` でなくscaleが `*` の場合
    - エラーになる(浮動小数点数に対して桁数を固定することは意味がない)

- precisionのみ指定されscaleが省略された場合は `0` が指定されたものとみなす
  - `DECIMAL(p)` == `DECIMAL(p, 0)`
- カッコが省略された場合は既定の最大桁の整数となる
  - `DECIMAL` == `DECIMAL(38, 0)`
- 「`DECIMAL(*,*)` がキャスト式のみで使用可能」という点以外は列定義とキャストで共通

## TsurugiのDECIMALの実装

### mpdecimalによるDECIMALの表現

DECIMALの表現方法は下記のように複数あり、実装は必要に応じて `decimal::Decimal` と `takatori::decimal::triple` を使い分ける

#### 構造体 `mpd_t`

- [mpdecimal C API](https://www.bytereef.org/mpdecimal/doc/libmpdec/index.html)で十進浮動小数点数を扱う構造体で、係数部分以外のデータをもつ
- `sizeof(mpd_t) == 48` ??
- SQL実行エンジンが直接扱うことはほとんどない

#### クラス `decimal::Decimal`

- [mpdecimal C++ API](https://www.bytereef.org/mpdecimal/doc/libmpdec++/index.html)で十進浮動小数点数を扱うクラス
- `mpd_t`とそれが指す係数データ領域(32バイト)を保持する
- `sizeof(decimal::Decimal) == 80` ??
- 自分の持つデータ領域へのポインタをもつためtrivially copyableではない
- NaN(signaling or quiet), +INF/-INF, 非正規化数(subnormal numbers)を保持することができる
- +0と-0は区別される

#### クラス `takatori::decimal::triple`

- jogasaki record_ref/record_metaで使用されるDECIMALの実行時型
- 符号部分、指数部分、係数部分からなる
  - 係数部分: (std:uint64_tのペアからなる)符号なし128ビット整数
  - 指数部分: std::int32_t
  - 符号部分: std::int64_t
- `mpd_uint128_triple_t`に相当
- `decimal::Decimal`がtrivially copyableでないためjogasakiはこれを実行時型としている
  - ただし `DECIMAL(*,*)` として使用する際は係数と指数は演算に使用するコンテキストオブジェクトに合わせて制限される
- `sizeof(takatori::decimal::triple) == 32`
- NaN, +INF/-INFは保持できない
- +0と-0は区別されない

### 表現可能な数の範囲

各型で表現可能なデータ範囲は下記の順に広くなる

`DECIMAL(p,s)` < `DECIMAL(*,*)` (≒ `takatori::decimal::triple` ) < `decimal::Decimal`

- `DECIMAL(p,s)`は桁数が`p`桁までに制限され、`p`の最大は38
- `DECIMAL(*,*)`は`triple`を38桁までに制限したもので、指数部分は38よりも広い範囲をとることができるため `DECIMAL(38, s)` (`s`は`[0,38]`の範囲の任意の値) よりも広い。
- `decimal::Decimal` はNaNやINFを含むうえ、係数部分のデータ領域が32バイトあるため `triple` (係数部分16バイト)よりも広い

## その他

- mpdecimalは可変長データの取り扱いが可能でC APIでは32バイトを超える係数を扱うことも可能だがSQL実行エンジンでは使用していない
  - C++ APIの `decimal::Decimal` は32バイト固定
- jogasakiがDECIMAL演算する場合は `decimal::IEEEContext(160)` のコンテキストをprecision 38桁に調整したものを使用し、範囲から逸脱する場合をエラーにする
  - `decimal::IEEEContext(128)` がdecimal128相当でありメジャーなフォーマットだが、precision 34桁までなので桁が足りない

