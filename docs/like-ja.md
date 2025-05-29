# LIKE句外部仕様

2025-05-29 nishimura

## この文書について

* この文書では`LIKE`(及び`ESCAPE`)句の外部仕様を示す。

## 前提

本文書中で言及する「文字列」はすべてUTF-8エンコーディングで表現された文字列を指す。

## LIKE句(及びESCAPE句)の構文

`LIKE like_expression [ ESCAPE escape_expression ]`

* like_expression、escape_expression、入力文字列のいずれかが NULL の場合、結果も NULL となる。
* like_expression：
  * 一致判定を行うパターン文字列。ワイルドカード文字 `%`（任意の文字列）および `_`（任意の1文字）を含む。
* escape_expression：
  * escape対象の文字、文字数は0or1でなければならない。文字数が2以上の場合`UNSUPPORTED_RUNTIME_FEATURE_EXCEPTION`を返す。
  * escape_expressionに指定しない場合、いかなる文字もESCAPE文字と解釈しない。


### 末尾のエスケープ処理

* 末尾文字がESCAPE文字であった場合、末尾から連続してESCAPE文字が奇数回連続した場合`UNSUPPORTED_RUNTIME_FEATURE_EXCEPTION`を返す。

#### 例

| 式                              | 結果または解釈                              |
|--------------------------------|---------------------------------------------|
| `LIKE 'abcdd' ESCAPE 'd'`      | `"abcd"` として解釈（`d` がエスケープ文字） |
| `LIKE 'abcddd' ESCAPE 'd'`     | `UNSUPPORTED_RUNTIME_FEATURE_EXCEPTION`

### ワイルドカードのエスケープ  

* %および_のワイルドカード文字がESCAPE文字として指定された場合、ワイルドカード文字ではなく単なるエスケープ文字として解釈される。

| 式                              | 解釈・マッチ対象                        |
|--------------------------------|-----------------------------------------|
| `LIKE 'abc%%' ESCAPE '%'`      | `"abc%"` と解釈され、`"abcd"` にはマッチしない |