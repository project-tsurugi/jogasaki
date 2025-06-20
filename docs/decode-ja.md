# DECODE 外部仕様

2025-06-20 nishimura

## この文書について

- この文書では `DECODE`の外部仕様を示す。

## 目次

1. [定義](#定義)
2. [外部仕様](#外部仕様)

---

## 定義

`DECODE` は、指定されたデコード形式に従って文字列をデコードし、その結果をバイナリデータ（オクテット列）として返す関数です。


```
DECODE(string_expression,encode_expression)
```

- `string_expression`：デコード対象の文字列
- `encode_expression` :デコード方式を示す文字列

## 外部仕様

- `Tsurugi`では`string_expression` の型として`char`、`varchar`の2つのみ許容されます。
- `Tsurugi`では`encode_expression` の型として`varchar`のみ許容されます。
- 現時点で指定可能なデコード方式は `base64` のみです。
- `string_expression` が `NULL` の場合、戻り値も `NULL` となります。
- `string_expression` が `base64`で指定可能な文字列でない場合エラーを返します。
- `DECODE`の戻り値の型は`varbinary`になります。
