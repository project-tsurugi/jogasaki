# ログシッピングにおける型サポート

2022-07-12 kurosawa(NT)

## この文書について

hayatsukiとjogasakiはlog recordのkey/value部分をmsgpackによってシリアライズされた形式で交換する。
本文書はサポートされる型のmsgpack上の型を記述する。

## 型の対応

SQLとTsurugiの型、[msgpackの型](https://github.com/msgpack/msgpack/blob/master/spec.md#type-system)の対応を下表に示す

| SQL     | Tsurugi           | msgpack           |
|---------|-------------------|-------------------|
| `INT`     | `int4`              | `Integer`または`Nil`(*1) |
| `BIGINT`  | `int8`              | `Integer`または`Nil`(*1)     |
| `REAL`    | `float4`            | `Float`または`Nil`(*1)       |
| `DOUBLE`  | `float8`            | `Float`または`Nil`(*1)       |
| `CHAR`    | `character`         | `String`または`Nil`(*1)      |
| `VARCHAR` | `character varying` | `String`または`Nil`(*1)      |

(*1) : `nil`(値がNULLであることを示す)を格納するときのみ`Nil`となる

