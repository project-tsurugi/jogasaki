# SUBSTRING動作仕様

2025-03-13 nishimura

## この文書について

* この文書では`SUBSTRING`動作仕様を示す

# 目次

1. [定義](#定義)
2. [動作仕様](#動作仕様)
3. [注意事項](#注意事項)
4. [早見表](#早見表)

## 定義

`SUBSTRING`関数の各種`expression`を以下の通り定義する

```
SUBSTRING(string_expression FROM start_position FOR length)
```

以下の略称を使用する

* SE - string_expression
* S  - start_position
* L  - length

`Tsurugi`では`string_expression` SEの型として`char`、`varchar`、`binary`、`varbinary`の4つのみ許容されます。

## 動作仕様

### NULLを返すケース

`NULLを返すケース`は以下の通りです。

* `UTF-8`として不正な文字列が存在する(対象文字列の型が`varchar`もしくは`char`のみ)
* 対象文字列が`NULL`
* `start_position` (S) < 1 または `SE`の長さ < `start_position`
* `length` (L) < 0

### 空文字を返すケース

空文字を返すケースは`NULLを返すケース以外`でかつ`length (L) = 0`

### 文字列を返すケース(典型的なパターン)

`NULLを返すケース`、`空文字を返すケース以外`はすべて文字列を返します。

以下に例を挙げて説明します。

対象文字列`"abcde"`を格納したテーブル`T1`の`C1`列に対して、`SUBSTRING`を使用して文字を取り出す典型的なSQL文は以下の通りです。

```
select SUBSTRING ( C1 FROM 2 FOR 3 ) FROM T1
```

この場合、2文字目の`b`を起点にして3文字目の`d`までの`"bcd"`文字列を取り出します。この動作は、PostgreSQL(14.12)、MariaDB(10.6.18)、Oracleの主要DBと同じです。

### 文字列を返すケース(可変長で文字数を超過するパターン)

`length` (L) が可変長で格納した文字数を超える場合、例えば`varchar(20)`型に`"abcde"`を格納して`SUBSTRING`、`start_position` (S) =3 `length` (L) =4 適用した場合、

```
select SUBSTRING ( C1 FROM 3 FOR 4 ) FROM T1
```

`c`からは3文字しかないため、`c`起点の文字数より大きい`length` (L) を指定したケースになりますが、この場合は指定可能な文字数の最大値を返します。

このケースの場合指定可能な文字数は`c`起点で`e`の位置までの文字数、すなわち`3`です。

```
select SUBSTRING ( C1 FROM 3 FOR 3 ) FROM T1
```

よって`"cde"`が返却されます。

### 文字列を返すケース(固定長で文字数を超過するパターン)

`length` (L) が固定長で指定した文字数を超える場合、例えば`char(20)`に格納された文字列に対して、`start_position = 3`、`length = 40`という指定があるとします。この場合、`length`の指定は実際に格納されている最大の文字数に調整されます。例の場合、`length = 17`が最大値となり、指定可能な範囲内で文字列が返されます。

## 注意事項

### 2,3,4バイト文字も文字数としては1カウント

`SUBSTRING`関数で指定する`start_position` (S) および`length` (L) は、文字数を基準にしているため、UTF-8 のような可変長エンコーディングでは、1文字が複数バイトになることがあります。このため、文字列のバイト数と文字数は一致しません。`start_position`と`length`は文字単位で指定することを意識してください。

特に`Tsurugi`では文字列は`UTF-8`として認識されていますので、例えば`"あ"`は3バイト文字ですが文字数としては1とカウントされます。

ただし、例外としてSEの型が`binary`と`varbinary`の場合のみバイナリ文字列として認識され1バイト1文字としてカウントされます。

### string_expressionの型で結果が異なる例

`varchar(20)`および`char(20)`に文字列`"abcde"`を格納した場合
varcharは可変長なので`"abcde"`という文字列扱いですが、charは固定長のため`"abcde               "`という風にeの後続に15個の空白文字が格納されています。

`start_position`を1、`length`を5と指定し場合、可変長、固定長両方とも`"abcde"`を返しますが

`start_position`を1、`length`を6と指定し場合、可変長は`abcde`、固定長は`"abcde "`を返すことに注意してください

## 早見表

`varchar(20)`に`"abced"`を格納した際の`SUBSTRING`の返却値は以下の通りです

| FROM | FOR  |返却値 |
| ---- | ---- | ---- |
|-1 |なし |NULL|
|0 |なし |NULL|
|1 |なし |abcde|
|2 |なし |bcde|
|3 |なし |cde|
|4 |なし |de|
|5 |なし |e|
|6 |なし |NULL|
|-1|-1|NULL|
|-1|0|NULL|
|-1|1|NULL|
|0|-1|NULL|
|0|0|NULL|
|0|1|NULL|
|1|-1|NULL|
|1|0|空文字|
|1|1|a|
|1|2|ab|
|1|3|abc|
|1|4|abcd|
|1|5|abcde|
|1|6|abcde|
|2|-1|NULL|
|2|0|空文字|
|2|1|b|
|2|2|bc|
|2|3|bcd|
|2|4|bcde|
|2|5|bcde|
|3|-1|NULL|
|3|0|空文字|
|3|1|c|
|3|2|cd|
|3|3|cde|
|3|4|cde|
|4|-1|NULL|
|4|0|空文字|
|4|1|d|
|4|2|de|
|4|3|de|
|5|-1|NULL|
|5|0|空文字|
|5|1|e|
|5|2|e|
|6|-1|NULL|
|6|0|NULL|
|6|1|NULL|
