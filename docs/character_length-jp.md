# UPPER LOWER外部仕様

2025-03-27 nishimura

## この文書について

* この文書では`CHARACTER_LENGTH` の外部仕様を示す 

# 目次

1. [定義](#定義)
2. [外部仕様](#外部仕様)
3. [付録](#付録)

## 定義

`CHARACTER_LENGTH` は、文字列の「文字数」を取得するために **ISO/IEC 9075** で定義された関数です。  
`Tsurugi` は **ISO/IEC 9075**（SQL標準）の `CHARACTER_LENGTH` の仕様に準拠しています。  


### 関数の構文

```
CHARACTER_LENGTH(string_expression)
CHAR_LENGTH(string_expression) -- CHARACTER_LENGTH のエイリアス
```

* `string_expression`：変換対象の文字列式

`CHARACTER_LENGTH` 関数は、`string_expression` に含まれる**文字の数**を返します。  


## 外部仕様

* `Tsurugi`では`string_expression` の型として`char`、`varchar`の2つのみ許容されます。
* `char`、`varchar` は UTF-8 データをコードポイント単位で処理し、1文字単位でカウントします。
* **不正なUTF-8シーケンスが入力された場合、0を返します**
* 入力値が `NULL` の場合、戻り値は `NULL`を返します。

