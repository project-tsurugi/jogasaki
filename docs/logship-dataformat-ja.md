# ログシッピングにおける型サポート

2022-07-12 kurosawa(NT)

## この文書について

hayatsukiとjogasakiはlog recordのkey/value部分をmsgpackによってシリアライズされた形式で交換する。
本文書はサポートされる型のmsgpack上の型を記述する。

## 型の対応

SQLとTsurugiの型、[msgpackの型](https://github.com/msgpack/msgpack/blob/master/spec.md#type-system)の対応を下表に示す

| SQL         | Tsurugi             | msgpack                 | 備考                                                                        |
|-------------|---------------------|-------------------------|---------------------------------------------------------------------------|
| `INT`       | `int4`              | `Integer`または`Nil`(*1)   ||
| `BIGINT`    | `int8`              | `Integer`または`Nil`(*1)   ||
| `REAL`      | `float4`            | `Float`または`Nil`(*1)     ||
| `DOUBLE`    | `float8`            | `Float`または`Nil`(*1)     ||
| `CHAR`      | `character`         | `String`または`Nil`(*1)    ||
| `VARCHAR`   | `character varying` | `String`または`Nil`(*1)    ||
| `DECIMAL`   | `decimal`           | `Binary`または`Nil`(*1)    | (*2)                                                                      |
| `DATE`      | `date`              | `Integer`または`Nil`(*1)   | `1970-01-01`からの経過日数 <br/> `1970-01-01`より前の日付の場合は負の値                       |
| `TIME`      | `time_of_day`       | `Integer`または`Nil`(*1)   | `00:00:00.000000000`からの経過ナノ秒 <br/> 0以上の値をとり、`23:59:59.999999999`に対応する値が最大 |
| `TIMESTAMP` | `time_point`        | `Timestamp`または`Nil`(*1) | Timestamp extension typeの96-bitフォーマットを使用                                  |

(*1) : `nil`(値がNULLであることを示す)を格納するときのみ`Nil`となる

(*2) : 以下の手順により得られるバイト列
1. 列のメタデータからprecision `p`とscale `s`を得る
2. 与えられたdecimalの値`x`を`x = y * 10^(-s)`として表す。(`y`は符号付き整数)
3. 任意の`p`桁の符号付き整数を2の補数で表現可能な最小のバイト数を`n`とする
4. 2の補数で符号付き整数`y`を表現したビックエンディアンな`n`バイトのバイト列を結果とする

