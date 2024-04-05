# 型変換メモ

2024-03-07 kurosawa

## 本文書について

型変換(に伴う値の変換)の仕様はtakatori文書の[キャスト][キャスト]に準じるが、その仕様詳細を記述する

## 仕様詳細

### 浮動小数へ変換時のunderflow/overflow

  - 変換先の型がfloat4またはfloat8で、値が `-std::numeric_limits::min()` と `std::numeric_limits::min()` の間(端点を含まない)の場合、underflowが発生する。この場合、変換結果は負又は正のゼロとする。
  - 変換先の型がfloat4またはfloat8で、値が `-std::numeric_limits::max()` 未満か `std::numeric_limits::max()` を越える場合overflowが発生する。この場合、変換結果は負又は正の無限大とする。

いずれの場合も精度は失われない(approx. numberへの変換では精度は失われない)。

### 特殊値の文字列表現

浮動小数点型(float4またはfloat8)の値を文字列型へ変換した場合の表現は下記の通り

  - 非数(not a number): `NaN`
    (負の符号をもつ非数も `NaN` に変換され `-NaN` とはならない)
  - 正の無限大: `Infinity`
  - 負の無限大: `-Infinity`
  - 正のゼロ: `0`
  - 負のゼロ: `-0`
  - その他: 適切な有限値を表す文字列(実装依存)
    - 指数表現を使用する場合、指数部のアルファベット`E`には大文字を使用する
    - 例: `1.23E-10`

    (非数や無限大を表す単語はPostgresqlやJava、mpdecimalの表現に合わせている。C++の`std::to_string`や`printf`は異なるので実装注意)

逆に文字列から浮動小数点型の特殊値(非数または無限大)へ変換する場合は次の規則に従う

  - 符号は省略可能。省略時は正の符号が指定されたものとして扱う
  - 符号を除いた部分が `NaN` または `INF` または `Infinity` とcase-insensitiveで等しい文字列のみが受け入れられる
  - 先頭や末尾の空白からなる文字列は無視される(文字列から変換時の一般的なルール)

### 文字列からの変換

  - 文字列から数値への変換の場合、基本的に文字列を `DECIMAL(*,*)` の値へ変換し、さらに変換先の型へ変換する、ただし下記の値の場合をのぞく

    - `Infinity`, `NaN`など特殊値を表す文字列は `DECIMAL(*,*)` に含まれない。この場合変換先の型によって挙動が異なる
      - 変換先が整数型やdecimalの場合、format error
      - 変換先が浮動小数型(float4/float8)の場合、文字列があらわす特殊値を結果とする
        - ただしNaNの符号は正のもののみを扱い、`-NaN` の変換結果は正の符号をもつNaNになる
      - `Infinity`, `NaN`などの特殊値を表す文字列から有限値を生成することはしない

    - `-0`, `-0.0` など負のゼロを表す文字列の場合
      - 変換先が浮動小数型(float4/float8)の場合、負のゼロを変換結果とする
      - それ以外の変換先の場合、通常のゼロを変換結果とする

  - 非特殊値を表す文字列の場合、桁数と指数が `DECIMAL(*,*)` の妥当な範囲内であることが必要
    - この範囲に収まらない場合は format error (扱える数値のフォーマットでない)

  - `DECIMAL(*,*)`からの変換は下記セクション参照

### 数値型の`DECIMAL(*,*)` への変換

  - float4/float8の特殊値
    - NaNはarithmetic error
    - Infinity/-Infinityは `DECIMAL(*,*)`の最大値・最小値になる

  - tripleへ収まらない場合には下記のように処理する
    - 係数部分の桁数が38桁を越える場合は末尾の桁を削り指数を増やす(truncate, 精度は失われる)
      - `DECIMAL(p,s)` の場合と異なり999...99への丸めは行わない
        - 999...99が最も近い数値とはかぎらないため
        - 例えば1000...00はreduceすれば桁数が1になる
    - 指数部分がtripleのサポートする範囲を越える場合はエラー
      - 変換でこれが起こる可能性があるのは文字列からの変換のみであり、format errorとする
      - 他の数値型は、指数の絶対値が比較的小さなものしか表現できない

### `DECIMAL(*,*)` の数値型への変換

- `DECIMAL(*,*)`の値が変換先の範囲に収まらない場合は、範囲の最大値・最小値を結果とする
  - 精度は失われる
  - 変換先がfloat4/float8の場合は+INF/-INFまたは+0.0/-0.0が結果となる。(参照[浮動小数へ変換時のunderflow/overflow](浮動小数へ変換時のunderflow/overflow))

- 変換先が `DECIMAL(p,s)` の場合、precision `p` と `s`に適合するように変換する。以下簡単のために正の数についてのみ述べる。
  - 整数部分が `p-s` 桁に収まらない場合は `99...999` ( `p-s`桁の整数部と`s`桁の小数部)を変換結果とする。精度は失われる。
  - 小数部分が `s` 桁に収まらない場合は収まるまで末尾の桁を `0`方向へroundingする(truncate)。末尾が0でない場合は精度が失われる。
      - 繰り上がりをすると桁あふれの処理が大変なので常に0方向へのrounding(truncate)とする

## 付録・補足情報

### 代入変換とキャスト変換

  - 値の変換ロジックは代入変換もキャスト変換も共通
  - 変換が許可される型の組がキャスト変換と代入変換で異なり、[キャスト][キャスト]と[代入変換][代入変換]の表に記述されている
    - 原則として代入変換は暗黙的に行われるので精度を失う代入変換はエラーになるようになっている

### 浮動小数点数と `DECIMAL(*,*)` の変換の実装

  - 浮動小数点数(float4/float8)は一旦文字列に変換してから上記手順で `DECIMAL(*,*)` へ変換する
    - mpdecimalが浮動小数点数と `decimal::Decimal` 間の変換をサポートしていないため
    - 実装メモ：
      - `DECIMAL(*,*)` からfloat4/float8 : `decimal::Decimal::to_sci` および `std::stof`/`std::stod` を使用する
      - float4/float8から `DECIMAL(*,*)` : `std::stringstream::operator<<` および `decimal::Decimal`コンストラクタを使用する
        - std::to_stringは不要なroundingなどの問題があるため使用しない

### 浮動小数点数の文字列表現

IEEE754-2008は浮動小数点数を文字列に変換可能であることを規定しているがその詳細なフォーマットは決めていない。mpdecimalが準拠する [General Decimal Arithmetic](https://www.speleotrove.com/decimal/) は詳細仕様を提供しており、jogasakiも文字列からの変換時や `DECIMAL`の文字列化はmpdecimal経由であり、これを利用している。

- 入力文字列のシンタックス
[Numeric string syntax](https://www.speleotrove.com/decimal/daconvs.html#refnumsyn)

- 出力文字列の生成ルール
[to-scientific-string – conversion to numeric string](https://speleotrove.com/decimal/daconvs.html#reftostr)

ただし、下記の点はGeneral Decimal Arithmeticから逸脱している

- signaling NaNのシンボル `sNaN` は受け付けない
- diagnostic codeつきNaNのシンボル `NaN0`等は受け付けない



[キャスト]: https://github.com/project-tsurugi/takatori/blob/master/docs/ja/scalar-expressions-and-types.md#%E3%82%AD%E3%83%A3%E3%82%B9%E3%83%88%E5%A4%89%E6%8F%9B

[代入変換]: https://github.com/project-tsurugi/takatori/blob/master/docs/ja/scalar-expressions-and-types.md#%E4%BB%A3%E5%85%A5%E5%A4%89%E6%8F%9B
