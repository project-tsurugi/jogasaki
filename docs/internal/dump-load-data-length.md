# dump/load 時の文字列・バイナリデータの長さに関するメモ

2024-08-19 kurosawa

## 本文書について

文字列・バイナリデータはその型に長さを持つことがあるが、ファイルのメタデータが長さを保持するとは限らないため、ダンプ・ロード処理の前後でその内容が変化することがある。その取扱について記述する。

## Parquet

### 対象となるParquet データ型

* Logical type `STRING`

  * 長さを保持しない
  * tsurugiの `char`, `varchar` に対応

* Primitive type `BYTE_ARRAY`

  * 長さを保持しない
  * tsurugiの `binary`, `varbinary` に対応

### 長さの取り扱い

* dump 

  Parquet側の対象データ型が長さを保持しないため、tsurugiの型における長さはダンプ後のファイルには反映されない

  | tsurugiの型 | Parquetの型 | 備考 |
  | --- | --- | --- |
  | `char(n)` | `STRING` | `n` に関する情報は保存されない|
  | `varchar(n)` | `STRING` |`n` に関する情報は保存されない |
  | `binary(n)` | `BYTE_ARRAY` |`n` に関する情報は保存されない |
  | `varbinary(n)` | `BYTE_ARRAY` |`n` に関する情報は保存されない |

* load

  * パラメータは長さを持たない可変長なデータ型、つまり `varchar(*)` または `varbinary(*)` として取り扱われる

  | Parquetの型 | tsurugiの型 | 備考 |
  | --- | --- | --- |
  | `STRING` | `varchar(*)` | |
  | `BYTE_ARRAY` | `varbinary(*)` | |

## Arrow

### 対象とするArrow データ型

* `Utf8`

  * 長さを保持しない
  * tsurugiの `char`, `varchar` に対応する

* `Binary`

  * 長さを保持しない
  * tsurugiの `varbinary` に対応する

* `FixedSizeBinary`

  * 長さを保持する
  * tsurugiの `binary` に対応する
    * オプション(*1)が設定された場合はダンプ時のみ `char` にも対応する

### 長さの取り扱い

* dump 

  * Arrow側の対象データ型が長さを保持する場合、tsurugiの型における長さはダンプ後のファイルに反映される
  * そうでない場合、tsurugiの型における長さはダンプ後のファイルには反映されない

  | tsurugiの型 | Arrowの型 | 備考 |
  | --- | --- | --- |
  | `char(n)` | `Utf8` | `n` に関する情報は保存されない|
  | `char(n)` | `FixedSizeBinary byteWidth:n` | オプション(*1)指定時のみ|
  | `varchar(n)` | `Utf8` |`n` に関する情報は保存されない |
  | `binary(n)` | `FixedSizeBinary byteWidth:n` | |
  | `varbinary(n)` | `Binary` |`n` に関する情報は保存されない |

* load

  * パラメータは長さを持たない可変長なデータ型、つまり `varchar(*)` または `varbinary(*)` として取り扱われる

  | Arrowの型 | tsurugiの型 | 備考 |
  | --- | --- | --- |
  | `Utf8` | `varchar(*)` | |
  | `Binary` | `varbinary(*)` | |
  | `FixedSizeBinary byteWidth:n` | `varbinary(*)` |`n` は考慮されない |

(*1) オプションの指定(`ArrowFileFormat::character_field_type`)により `char(n)` のdump時の型を `FixedSizeBinary byteWidth:n` とすることが可能(PG-Strom連携用)。