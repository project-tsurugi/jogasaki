# ENCODE 外部仕様

2025-06-20 nishimura

## この文書について

- この文書では `ENCODE`の外部仕様を示す。

## 目次

1. [定義](#定義)
2. [外部仕様](#外部仕様)

---

## 定義

`ENCODE` は、指定されたエンコード形式に従って文字列をエンコードし、その結果を文字列として返す関数です。


```
ENCODE(string_expression,encode_expression)
```

- `string_expression`：エンコード対象の文字列
- `encode_expression` :エンコード方式を示す文字列

## 外部仕様

- `Tsurugi`では`string_expression` の型として`binary`、`varbinary`の2つのみ許容されます。
- `Tsurugi`では`encode_expression` の型として`varchar`のみ許容されます。
- 現時点で指定可能なエンコード方式は 'base64' のみです。
- `string_expression` が `NULL` の場合、戻り値も `NULL` となります。
- `ENCODE`の戻り値の型は`varchar`になります。
- エンコードは [RFC 4648](https://datatracker.ietf.org/doc/html/rfc4648) に基づく標準的な Base64 エンコードで、末尾のパディング文字 `=` を含みます。
