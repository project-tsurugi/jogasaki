# ダンプ/ロード入出力ファイル仕様

2022-04-15 kurosawa(NT)

2022-06-28 kurosawa(NT) ダンプファイル名・結果セットフォーマットを追加

## この文書について

ダンプ/ロード機能が入出力を行うファイルの仕様を記述する

## 仕様

### ファイルフォーマット

- ダンプ/ロードによって読み書きされるファイルはApache Parquetのファイルフォーマット([FF])に従うものとする
  - Tsurugi が取り扱えるデータ型は、 [FF] に含まれるもののうち、下記 [データ型](#データ型) に記載されたもののみとする
  - [データ型](#データ型) に記載されないデータや値については、ダンプファイル中に正しく保存できないものとする
    - このような場合、文字列等の互換性のあるデータ型に変換し、格納することを推奨する
- SQLで扱うレコードを同じ列の順序で格納する

### データ型

- 標準SQLとTsurugi、Parquetの型の対応を下表に示す
- Parquetの型でLogical Type(*0)を持つものはその属性も併せて示す

| SQL                         | Tsurugi           | Parquet                                                                                                    | 備考    |
|-----------------------------|-------------------|------------------------------------------------------------------------------------------------------------|-------|
| BOOLEAN                     | boolean           | Primitive Type: BOOLEAN                                                                                    |       |
| TINYINT                     | int1              | Primitive Type: INT32<br>Logical Type: INT <br> bit width: 8 <br> signed: true                             |       |
| SMALLINT                    | int2              | Primitive Type: INT32<br>Logical Type: INT <br> bit width: 16 <br> signed: true                            |       |
| INT                         | int4              | Primitive Type: INT32<br>Logical Type: INT <br> bit width: 32 <br> signed: true                            |       |
| BIGINT                      | int8              | Primitive Type: INT64<br>Logical Type: INT <br> bit width: 64 <br> signed: true                            |       |
| REAL                        | float4            | Primitive Type: FLOAT                                                                                      |       |
| DOUBLE                      | float8            | Primitive Type: DOUBLE                                                                                     |       |
| CHAR                        | character         | Primitive Type: BYTE_ARRAY <br>Logical Type: STRING                                                        |       |
| VARCHAR                     | character varying | Primitive Type: BYTE_ARRAY <br>Logical Type: STRING                                                        |       |
| DECIMAL(p,s)                | decimal           | Primitive Type: BYTE_ARRAY <br>Logical Type: DECIMAL<br> precision: p <br> scale: s                        |       |
| DATE                        | date              | Primitive Type: INT32 <br>Logical Type: DATE                                                               |       |
| TIME                        | time_of_day       | Primitive Type: INT64 <br>Logical Type: TIME <br> utc adjustment: false<br>unit: NANOS                     | (*1)  |
| TIME WITH TIME ZONE         | time_of_day       | Primitive Type: INT64 <br>Logical Type: TIME <br> utc adjustment: true<br>unit: NANOS                      | (*2)  |
| TIMESTAMP                   | time_point        | Primitive Type: INT64 <br>Logical Type: TIMESTAMP <br> utc adjustment: false<br> unit: MILLIS/MICROS/NANOS | (*1)  |
| TIMESTAMP WITH TIME ZONE    | time_point        | Primitive Type: INT64 <br>Logical Type: TIMESTAMP <br> utc adjustment: true<br> unit: MILLIS/MICROS/NANOS  | (*2)  |
| INTERVAL                    | datetime_interval | Primitive Type: FIXED_LEN_BYTE_ARRAY (length=12) <br> Logical Type: INTERVAL                               |       |
| BIT                         | bit               | 対応なし                                                                                                       |       |
| BIT VARYING                 | bit varying       | 対応なし                                                                                                       |       |
| BINARY                      | octet             | Primitive Type: BYTE_ARRAY                                                                                 |       |
| VARBINARY                   | octet varying     | Primitive Type: BYTE_ARRAY                                                                                 |       |
| ARRAY                       | array             | LIST                                                                                                       | (*3)  |
| ROW                         | record            | TBD                                                                                                        | (*4)  |

(*0) Parquetのデータ型はPrimitive Type([TY])とLogical Type([LT])で表される。詳細はリンク先の文書を参照。

(*1) ローカルタイムとして日時を記録する

(*2) UTCとして日時を記録する (タイムゾーンオフセット情報は含められない)

(*3) 要素型は、上記表に含まれるいずれかのデータ型でなければならない

(*4) 2022/9 現在、詳細は検討中

### ヌルの表現

- ファイル内のレコードの全ての列はヌルを格納することができる
  - すなわちSchemaElementのrepetition_typeはrequired以外の値([MD])
- ヌルの表現方法もParquetの仕様に準じる([NU])

### 列名

Parquetファイルフォーマットに従い、列名はファイルにメタデータとして保存される(SchemaElementのnameフィールド([MD]))。
  - ダンプはクエリの出力の列名をここに保存する
  - ロードはこの列名と同名のホスト変数を利用し、ファイル内の列の値をSQL文で利用することができる

### 圧縮方式・エンコーディング

Parquetは複数の圧縮方式([CO])や列データのエンコーディング([EC])をサポートする。当文書ではこれらについては規定しない。
- Parquetファイルフォーマットに従い圧縮方式や列データのエンコーディングが正しくメタデータとして保存されている限りは任意の圧縮方式・エンコーディングを使用してよい(ColumnMetaDataのcodecやencodingsフィールドなど([MD]))。

### 暗号化

Parquetは暗号化ファイルの処理が可能([EN])だが、現在の計画ではダンプ/ロードは暗号化ファイルをサポートする予定はない

### ダンプファイル名

- ダンプファイルは全て`.parquet`の拡張子をもつ
- ダンプが複数ファイルを出力し、その順序に意味がある場合(例えばORDER BY句を利用したクエリによるダンプなど)、ファイル名は`<prefix>_<sequence number>.parquet`の形式とする。
  - `<prefix>`はダンプの実行ごとに生成される文字列
  - `<sequence number>`は昇順のシーケンス番号で0以上の整数値。これによって出力ファイルの順序を決められる
- それ以外の場合はダンプファイル名は規定されない(拡張子を除く)

## ダンプ実行結果セットフォーマット

tsubakuro経由でのダンプ実行(Transaction.executeDump())においては結果セット(ResultSet)へダンプされたファイルのパスが出力される。
この結果セットのレコードフォーマットは下記の通り。

- 結果セットのレコードは単一の列からなる
- 列名は不定
- 列のタイプは文字列型(VARCHAR)

## Non Logical Type のサポート(ロード)

上記[データ型](#データ型)の多くはParquetのLogical Typeを持つが、サードパーティ製品(Apache Drill等)によって作成されたParquetファイルは必ずしもこの限りではない。この場合、ロード時にAPI経由でplaceholderの型を適切に与える事でこれらの型についてもロードが可能である。この使用法が可能なParquetの型と、そのロード時に指定すべきplaceholderの型の対応を下表に示す

| Parquet  | Tsurugi           |  備考    |
|-------------|----------|----------------|
| Primitive Type: INT32<br>Logical Type: None  | int4              |        |
| Primitive Type: INT64<br>Logical Type: None  | int8              |        |

## Parquet前提バージョン

本提書の記述はApache Arrow 7.0.0に含まれるParquetを前提としている
https://arrow.apache.org/release/7.0.0.html
(ParquetはApache Arrowの一部に組み込まれている)

ダウンロード・インストール方法などは下記を参照
https://arrow.apache.org/install/

## リファレンス

[FF]
[MD]
[CO]
[EC]
[EN]
[NU]
[TY]
[LT]

[FF]:https://parquet.apache.org/docs/file-format/
[MD]: https://parquet.apache.org/docs/file-format/metadata/
[CO]: https://github.com/apache/parquet-format/blob/master/Compression.md
[EC]: https://github.com/apache/parquet-format/blob/master/Encodings.md
[EN]: https://github.com/apache/parquet-format/blob/master/Encryption.md
[NU]: https://github.com/apache/parquet-format#nulls
[TY]: https://github.com/apache/parquet-format#types
[LT]: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md