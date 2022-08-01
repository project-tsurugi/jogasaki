# ダンプ/ロード入出力ファイル仕様

2022-04-15 kurosawa(NT)

2022-06-28 kurosawa(NT) ダンプファイル名・結果セットフォーマットを追加

## この文書について

ダンプ/ロード機能が入出力を行うファイルの仕様を記述する

## 仕様

### ファイルフォーマット

- ダンプ/ロードによって読み書きされるファイルはApache Parquetのファイルフォーマット([FF])に従うものとする
- SQLで扱うレコードを同じ列の順序で格納する

### 型

- 標準SQLとTsurugi、Parquetの型の対応を下表に示す
- Parquetの型でLogical Type(*0)を持つものはその属性も併せて示す

| SQL          | Tsurugi           | Parquet                                                                                      | 備考       |
|--------------|-------------------|----------------------------------------------------------------------------------------------|----------|
| BOOLEAN      | boolean           | Primitive Type: BOOLEAN                                                                      | (*1)     |
| TINYINT      | int1              | Primitive Type: INT32<br>Logical Type: INT <br> bit width: 8 <br> signed: true               | (*1)     |
| SMALLINT     | int2              | Primitive Type: INT32<br>Logical Type: INT <br> bit width: 16 <br> signed: true              | (*1)     |
| INT          | int4              | Primitive Type: INT32<br>Logical Type: INT <br> bit width: 32 <br> signed: true              |          |
| BIGINT       | int8              | Primitive Type: INT64<br>Logical Type: INT <br> bit width: 64 <br> signed: true              |          |
| REAL         | float4            | Primitive Type: FLOAT                                                                        |          |
| DOUBLE       | float8            | Primitive Type: DOUBLE                                                                       |          |
| CHAR         | character         | Primitive Type: BYTE_ARRAY <br>Logical Type: STRING                                          |          |
| VARCHAR      | character varying | Primitive Type: BYTE_ARRAY <br>Logical Type: STRING                                          |          |
| DECIMAL(p,s) | decimal           | Primitive Type: BYTE_ARRAY <br>Logical Type: DECIMAL<br> precision: p <br> scale: s          |          |
| DATE         | date              | Primitive Type: INT32 <br>Logical Type: DATE                                                 |          |
| TIME         | time_of_day       | Primitive Type: INT64 <br>Logical Type: TIME <br> utc adjustment: true<br>unit: NANOS        |          |
| TIMESTAMP    | time_point        | Primitive Type: INT64 <br>Logical Type: TIMESTAMP <br> utc adjustment: true<br> unit: NANOS  |          |
| INTERVAL     | datetime_interval | TBD                                                                                          | (*2)     |
| BIT          | bit               | TBD                                                                                          | (*2)     |
| BIT VARYING  | bit varying       | TBD                                                                                          | (*2)     |
| ARRAY        | array             | TBD                                                                                          | (*2)     |
| ROW          | record            | TBD                                                                                          | (*2)     |
| (該当なし)       | octet             | TBD                                                                                          | (*2)(*3) |
| (該当なし)       | octet varying     | TBD                                                                                          | (*2)(*3) |

(*0) Parquetのデータ型はPrimitive Type([TY])とLogical Type([LT])で表される。詳細はリンク先の文書を参照。

(*1) 2022/4現在、未実装のため詳細は変更の可能性あり

(*2) 2022/4現在、詳細は検討中

(*3) 標準SQLの型ではないがPostgreSQLのバイナリ列データ型(bytea)に対応し、バイト列をchar/varcharに似た方式で扱えるようにするもの

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