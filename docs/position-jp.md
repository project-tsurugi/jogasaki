# `POSITION` 外部仕様

2025-04-02 nishimura

## この文書について

この文書では `POSITION` の外部仕様を示す。

# 目次

1. [定義](#定義)
2. [外部仕様](#外部仕様)
3. [付録](#付録)


## 定義

`POSITION` は、文字列内で特定の部分文字列が最初に出現する位置を取得するために **ISO/IEC 9075** で定義された関数である。
`Tsurugi` は **ISO/IEC 9075**（SQL標準）の `POSITION` の仕様に準拠している。

### 関数の構文

```
POSITION(substring IN string_expression)
```

* `substring`：検索する部分文字列
* `string_expression`：検索対象の文字列

`POSITION` 関数は、`substring` が `string_expression` 内で最初に現れる位置を **1-based index** で返す。
存在しない場合は `0` を返す。

## 外部仕様

* `Tsurugi` では `substring`、`string_expression` の型として `char`、`varchar` の2つのみ許容される。
* `POSITION` は UTF-8 データをコードポイント単位で処理する。**ただし将来的に変更になる可能性がある。**
* `string_expression` に **不正な UTF-8 シーケンスが入力された場合、NULL を返す。**
* `substring` に **不正な UTF-8 シーケンスが入力された場合、戻り値は不定である。**
* `substring` が `string_expression` 内に存在しない場合、戻り値は `0` を返す。
* `substring` または `string_expression` が `NULL` の場合、戻り値は `NULL` を返す。
* `substring` が `空文字` の場合戻り値は `1`を返す。

## 付録

コードポイントについては、[Unicode Code Points](https://unicode.org/glossary/#code_point) を参照のこと。
