# SUBSTRING外部仕様

2025-03-13 nishimura

## この文書について

* この文書では`SUBSTRING`の外部仕様を示す

# 目次

1. [定義](#定義)
2. [外部仕様](#外部仕様)
3. [付録](#付録)

## 定義

`SUBSTRING`は、文字列から部分文字列を抽出するためISO/IEC 9075 で定義された関数です。


`SUBSTRING`関数の構文は以下の通りです。

```
SUBSTRING(string_expression FROM start_position [FOR length])
```

* string_expression：部分文字列を抽出する対象の文字列式
* start_position：抽出を開始する位置を指定する整数式で、文字列の最初の文字を位置1とします。
* length：抽出するUTF-8のコードポイント数を指定する整数式（省略可能）

## 外部仕様

`Tsurugi`では`string_expression` の型として`char`、`varchar`、`binary`、`varbinary`の4つのみ許容されます。

`binary`、`varbinary`の場合はバイナリデータをバイト単位で処理します。戻り値の型は`varbinary`になります。
`char`、`varchar`はUTF-8データをコードポイント数単位で処理します。戻り値の型は`varchar`になります。

以下に具体的にどのような文字列を返すについて述べます。

### 文字列を返すケース(典型的なパターン)

`SUBSTRING`関数は `string_expression` の `start_position` 位置から `length` だけ部分文字列を取得します

以下に例を挙げて説明します。

```
SUBSTRING ( 'abcde' FROM 2 FOR 3 )
```

この場合、2文字目の`b`を起点にして3文字目の`d`までの`"bcd"`文字列を取り出します。この動作は、PostgreSQL(14.12)、MariaDB(10.6.18)、Oracleの主要DBと同じです。

### 有効コードポイント数を超えるパターン

`start_position`と`length`の合計値が`string_expression`の`有効コードポイント数`を超える場合、`length`に`有効コードポイント数` - `start_position`が指定されたと見なします。

### 特殊値を返すケース

#### NULLを返すケース

以下の場合は`NULL`を返します

* `UTF-8`として不正な文字列が存在する(対象文字列の型が`varchar`もしくは`char`のみ)
*  `string_expression`, `start_position`, `length`のいずれかがNULL
* `start_position`< 1 または `string_expression`の長さ < `start_position`
* `length` < 0

#### 空文字を返すケース

`NULL`を返すケース以外でかつ`length = 0`の場合、空文字列を返します


## 付録

### 早見表

`varchar(20)`に`"abced"`を格納した際の`SUBSTRING`の返却値は以下の通りです

| FROM | FOR  |返却値 |
| ---- | ---- | ---- |
|NULL|なし |NULL|
|1 |NULL |NULL|
|NULL |NULL |NULL|
|NULL |1 |NULL|
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

### string_expressionの型で結果が異なる例

`varchar(20)`および`char(20)`に文字列`"abcde"`を格納した場合
`varchar`は可変長なので`"abcde"`という文字列扱いですが、`char`は固定長のため`"abcde               "`という風にeの後続に15個の空白文字が格納されています。

`start_position`を1、`length`を5と指定し場合、`varchar`,`char`両方とも`"abcde"`を返しますが`start_position`を1、`length`を6と指定し場合、`varchar`は`abcde`、`char`は`"abcde "`を返すことに注意してください。

パディングについては詳細は
[charデータタイプのパディング](https://github.com/project-tsurugi/jogasaki/blob/master/docs/internal/char_padding.md) を参照してください。