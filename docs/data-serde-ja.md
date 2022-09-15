# Tsubakuro Result Set Ser/De format

2022-05-03 arakawa (NT)

## この文書について

* `jogasaki` - `tsubakuro` 間で result set のデータを授受する際のフォーマット

## コンセプト

* Tsurugi DB の全てのデータ型に対応
* 値にデータ型を含む
* データ型を先頭1バイトで識別可能
* エラー検出等は考えない

### Message Pack でない理由

* API の取り回しが悪く、複合型近辺にミスマッチがある
* Tsurugi DB のデータ型にネイティブ対応させるのが困難で、情報が落ちる

## データ形式

### ヘッダ

* 各エントリは 1 バイトのヘッダを有し、ヘッダによって形式を識別可能
* `0x00-e7` の範囲はヘッダに値の一部が含まれる

| ヘッダ値 | 型 | 概要 | 備考 |
|:-:|:--|:--|:--
| `0x00-3f` | `int` | 整数 (`[0,64)`) | 値埋め込み
| `0x40-7f` | `character` | 文字列 (オクテット数 `(0,64]`) | 要素数埋め込み
| `0x80-9f` | `row` | 行 (要素数 `(0,32]`) | 要素数埋め込み
| `0xa0-bf` | `array` | 配列 (要素数 `(0, 32]`) | 要素数埋め込み
| `0xc0-cf` | `int` | `[-16,0)` の整数 | 値埋め込み
| `0xd0-df` | `octet` | オクテット列 (要素数 `(0, 16]`) | 要素数埋め込み
| `0xe0-e7` | `bit` | ビット列 (要素数 `(0, 8]`) | 要素数埋め込み
| `0xe8` | `unknown` | `NULL`
| `0xe9` | `int` | 整数 (`[-2^63, +2^63-1])`)
| `0xea` | `float4` | binary32
| `0xeb` | `float8` | binary64
| `0xec` | `decimal` | 10進数 (係数部が `[-2^63,+2^63)`)
| `0xed` | `decimal` | 10進数
| `0xee` | `time_of_day (with_offset=true)` | タイムゾーンオフセット情報付きの時刻
| `0xef` | `time_point (with_offset=true)` | タイムゾーンオフセット情報付きの特定時点
| `0xf0` | `character` | UTF-8 文字列
| `0xf1` | `octet` | オクテット列
| `0xf2` | `bit` | ビット列
| `0xf3` | `date` | 日付
| `0xf4` | `time_of_day (with_offset=false)` | タイムゾーンオフセット情報なしの時刻
| `0xf5` | `time_point (with_offset=false)` | タイムゾーンオフセット情報なしの特定時点
| `0xf6` | `datetime_interval` | 日時区間
| `0xf7` | _reserved_
| `0xf8` | `row` | 行
| `0xf9` | `array` | 配列
| `0xfa` | `clob` | character large object reference | データそのものは含まない
| `0xfb` | `blob` | binary large object reference | データそのものは含まない
| `0xfc` | _reserved_
| `0xfd` | _reserved_
| `0xfe` | `end_of_contents` | end of contents
| `0xff` | _reserved_

### ペイロード

* 各エントリは、 1 バイトのヘッダの直後に、0 バイト以上のペイロードで値を表す
* 以下のような表記を用いる
  * `V` - バイト列 `V`
  * `L=V` - バイト列 `V` に `L` というラベルを付与
* バイト列は以下のいずれかで表す
  * `#xx` - 1 バイト即値
  * `b(N)` - `N` バイトの列
  * `d(N)` - `N` ビットの列 (8ビット単位で padding)
  * `e(N)` - `N` 個のエントリ
  * `sint` - 可変長符号付き整数 (`[-2^63,+2^63)`)
  * `uint` - 可変長符号なし整数 (`[0,+2^64)`)
* 表の「シーケンス」はヘッダバイトも含む
  * ヘッダ内に値の一部が埋め込まれている場合があるため

| ヘッダ値 | 型 | シーケンス | 値 |
|:-:|:--|:--|:--
| `0x00-3f` | `int` | `H=b(1)` | `H` の数値
| `0x40-7f` | `character` | `H=b(1), s=b(H-0x40+1)` | `s` からなる UTF-8 文字列
| `0x80-9f` | `row` | `H=b(1), s=e(H-0x80+1)` | `s` からなる行
| `0xa0-bf` | `array` | `H=b(1), s=e(H-0xa0+1)` | `s` からなる配列
| `0xc0-cf` | `int` | `H=b(1)` | `H-0xd0` の数値
| `0xd0-df` | `octet` | `H=b(1), s=b(H-0xd0+1)` | `s` からなるオクテット列
| `0xe0-e7` | `bit` | `H=b(1), s=d(H-0xe0+1)` | `s` からなるビット列
| `0xe8` | `unknown` | `#e8`
| `0xe9` | `int` | `#e9, v=sint` | `v` の数値
| `0xea` | `float4` | `#ea, s=b(4)` | `s` からなる binary32
| `0xeb` | `float8` | `#eb, s=b(8)` | `s` からなる binary64
| `0xec` | `decimal` | `#ec, e=sint, v=sint` | `v * 10^e`
| `0xed` | `decimal` | `#ed, e=sint, n=uint, v=b(n)` | v を big-endian の多倍長符号付き整数とみなして `v * 10^e`
| `0xee` | `time_of_day (with_offset=true)` | `#ee, v=uint, o=sint` | `00:00:00 ±hh:mm` にナノ秒 `v` を足したもの( `±hh:mm` はタイムゾーンオフセットを表す文字列で分 `o` を時 `hh` と分 `mm` に分解したもの)
| `0xef` | `time_point (with_offset=true)` | `#ef, e=sint, n=uint, o=sint` | `1970-01-01 00:00:00 ±hh:mm` からの秒数 `e` とナノ秒 `n`( `±hh:mm` はタイムゾーンオフセットを表す文字列で分 `o` を時 `hh` と分 `mm` に分解したもの)
| `0xf0` | `character` | `#f0, n=uint, s=b(n)` | `s` からなる UTF-8 文字列
| `0xf1` | `octet` | `#f1, n=uint, s=b(n)` | `s` からなるオクテット列
| `0xf2` | `bit` | `#f2, n=uint, s=d(n)` | `s` からなるビット列
| `0xf3` | `date` | `#f3, v=sint` | `1970-01-01` に日数 `v` を足したもの
| `0xf4` | `time_of_day (with_offset=false)` | `#f4, v=uint` | `00:00:00` にナノ秒 `v` を足したもの
| `0xf5` | `time_point (with_offset=false)` | `#f5, e=sint, n=uint` | `1970-01-01 00:00:00` からの秒数 `e` とナノ秒 `n`
| `0xf6` | `datetime_interval` | `#f6, y=sint, m=sint, d=sint, t=sint` | `y` 年 `m` 月 `d` 日 `v` ナノ秒
| `0xf7` | _reserved_ |`#f7`
| `0xf8` | `row` |`#f8, n=uint, s=e(n)` | `s` からなる行
| `0xf9` | `array` | `#f9, n=uint, s=e(n)` | `s` からなる配列
| `0xfa` | `clob` | `#fa, s=b(16)` | 識別子 `s` の CLOB
| `0xfb` | `blob` | `#fb, s=b(16)` | 識別子 `s` の BLOB
| `0xfc` | _reserved_ | `#fc`
| `0xfd` | _reserved_ | `#fd`
| `0xfe` | `end_of_contents` | `#fe` | 値の列の明示的な終端
| `0xff` | _reserved_ | `#ff`

### EOFの取り扱い

* result setの送受信用のストリーム終端には無限個の END_OF_CONTENTS が並んでいるとみなす

### `int` の取り扱い

* `int` は `takatori` 上の次の型を表すことができる
  * `boolean` - `0` が `false`, `1` が `true`
  * `int4`
  * `int8`
  * `decimal` - `scale=0` かつ `[-2^63,+2^63)` の範囲のみ

### 可変長整数

* [ペイロード] の項に出現した `uint`, `sint` はそれぞれ最大 9 バイトでエンコードされる可変長の整数である
* `uint`
  * Base-128 をもとに、以下の変更を加える
    * LSB から順に並べる
    * MSB を含むバイトだけ 8 ビットでグループ化する (`7*8 + 8*1 = 64` となる)
* `sint`
  * `(n << 1) ^ (n >> 63)` してから `uint` と同様にエンコードする

[ペイロード]:#ペイロード

### ビット/バイトオーダー

* 可変長整数を除き、基本的にビッグエンディアンを基本とする
  * `float4`, `float8`, `decimal` はそれぞれ先頭バイトに sign bit が来る
* `character`, `octet` は先頭のオクテットから順に並べる
* `bit` は先頭から8要素ずつグループ化してオクテットを構成し、順に並べる
  * 8要素のグループは、先頭要素から順に LSB から MSB へ配置する
  * 末尾のグループが8要素に満たない場合、 LSB から順に並べ、不足分は 0 で埋める

## リレーションの構造

* notation
  * `<name>` - 非終端記号
  * `NAME` - 終端記号、各エントリが該当する
  * `<element> ::= rule1 | rule2 | ... ;` - `<element>` は `rule1`, `rule2`, ... を導出可能
  * `element1 element2` - element1 に続いて element2 が出現
  * `element*` - element の0回以上の繰り返し
  * `element{N}` - element の `N` 回の繰り返し
  * `ELEMENT@N` - 値が `N` のエントリ (`ARRAY` または `ROW` 向け)

```bnf
<relation> ::= <row>* END_OF_CONTENTS

<value> ::= <atom>
         |  <collection>

<atom> ::= UNKNOWN
        |  INT
        |  FLOAT4
        |  FLOAT8
        |  DECIMAL
        |  CHARACTER
        |  OCTET
        |  BIT
        |  DATE
        |  TIME_OF_DAY
        |  TIME_POINT
        |  DATETIME_INTERVAL
        |  CLOB
        |  BLOB

<collection> ::= <row>
              |  <array>

<row> ::= ROW@N <value>{N}

<array> ::= ARRAY@N <value>{N}
```

## メモ

* decimal
  * `0xed` の `v` は、 `new BigInteger(byte[])`, `BigInteger.toByteArray()` と互換
