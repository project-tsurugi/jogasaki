# RTRIM 外部仕様

2025-06-23 nishimura

## この文書について

- この文書では `RTRIM`の外部仕様を示す。

## 目次

1. [定義](#定義)
2. [外部仕様](#外部仕様)

---

## 定義

`RTRIM` は、指定された文字列の末尾から、`ASCII半角スペース（U+0020）`のみを削除した結果を文字列として返す関数です。


```
RTRIM(string_expression)
```

- `string_expression`：トリム対象となる文字列

## 外部仕様

- `Tsurugi`では`string_expression` の型として`char`、`varchar`の2つのみ許容されます。
- `string_expression` が `NULL` の場合、戻り値も `NULL` となります。
- `RTRIM`の戻り値の型は`varchar`になります。
- 不正なUTF-8シーケンスが入力された場合、`不定な値`を返すことがあります。