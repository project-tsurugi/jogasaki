# UPPER LOWER外部仕様

2025-03-26 nishimura

## この文書について

* この文書では`UPPER`および`LOWER`の外部仕様を示す

# 目次

1. [定義](#定義)
2. [外部仕様](#外部仕様)
3. [付録](#付録)

## 定義

`UPPER`および`LOWER`は、文字列の大文字・小文字を変換するために **ISO/IEC 9075** で定義された関数です。`Tsurugi`は **ISO/IEC 9075**（SQL標準）の`UPPER`および`LOWER`の仕様に準拠していますが **Basic Latin（U+0041～U+005A, U+0061～U+007A）** 以外の変換に対応していません。

### 関数の構文

```
UPPER(string_expression)
LOWER(string_expression)
```

* `string_expression`：変換対象の文字列式

`UPPER`関数は、`string_expression` の **Basic Latin（U+0041～U+005A, U+0061～U+007A）** のアルファベットを**大文字に変換します。

`LOWER`関数は、`string_expression` の **Basic Latin（U+0041～U+005A, U+0061～U+007A）** のアルファベットを小文字に変換します。

## 外部仕様

* `Tsurugi`では`string_expression` の型として`char`、`varchar`の2つのみ許容されます。
* `char`、`varchar`はUTF-8データをコードポイント単位で処理し、 **Basic Latin（U+0041～U+005A, U+0061～U+007A）** のみ変換を行います。戻り値の型は`varchar`になります。不正なUTF-8シーケンスが入力された場合、関数の挙動は`不定な値`を返すことがあります。この場合、関数はエラーを発生させることありませんが出力結果に対して保証はありません。
* 入力値が `NULL` の場合、戻り値も `NULL` となります。


## 付録

* Unicodeのケース変換の詳細は、[Unicode Case Mapping](https://unicode.org/charts/)を参照。


