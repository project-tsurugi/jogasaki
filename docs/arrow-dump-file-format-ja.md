# Arrowダンプファイル仕様

2023-12-26 kurosawa(NT)

## この文書について

Arrowファイルのダンプ機能追加に伴い出力Arrowファイルの仕様を記述する

## 仕様

Arrowファイルを利用した際の仕様を下記に記述する。
ファイルフォーマット以外のダンプ・ロード機能については[PD]と共通

### ファイルフォーマット

- ダンプによって書き出されるApache Arrowファイルは[FF]に従うものとする
  - Tsurugi が取り扱えるデータ型は、 [FF] に含まれるもののうち、下記 [データ型](#データ型) に記載されたもののみ
  - [データ型](#データ型) に記載されないデータや値については、ダンプファイル中に正しく保存できない

### データ型

- 標準SQLとTsurugi、Arrowの型の対応を下表に示す

| SQL                         | Tsurugi           | Arrow (*0) | 備考    |
|-----------------------------|-------------------|--------------|-------|
| BOOLEAN                     | boolean           | Bool |       |
| TINYINT                     | int1              | Int bitWidth:8 is_signed:true |       |
| SMALLINT                    | int2              | Int bitWidth:16 is_signed:true |       |
| INT                         | int4              | Int bitWidth:32 is_signed:true |       |
| BIGINT                      | int8              | Int bitWidth:64 is_signed:true |       |
| REAL                        | float4            | FloatingPoint precision:SINGLE |       |
| DOUBLE                      | float8            | FloatingPoint precision:DOUBLE |       |
| CHAR(n)                     | character         | Utf8 | (*6)  |
| VARCHAR(n)                  | character varying | Utf8 |       |
| DECIMAL(p,s)                | decimal           | Decimal precision:p scale:s bitWidth:128 |       |
| DATE                        | date              | Date unit:DAY |       |
| TIME                        | time_of_day       | Time unit:NANOSECOND bitWidth:64 | (*1)  |
| TIME WITH TIME ZONE         | time_of_day       | Time unit:NANOSECOND bitWidth:64 | (*2) (*5) |
| TIMESTAMP                   | time_point        | Timestamp unit:MILLISECOND/MICROSECOND/NANOSECOND timezone:empty | (*1)  |
| TIMESTAMP WITH TIME ZONE    | time_point        | Timestamp unit:MILLISECOND/MICROSECOND/NANOSECOND timezone:UTC | (*2)  |
| INTERVAL                    | datetime_interval | TBD |       |
| BIT                         | bit               | 対応なし |       |
| BIT VARYING                 | bit varying       | 対応なし |       |
| BINARY(n)                   | octet             | FixedSizeBinary byteWidth:n |       |
| VARBINARY(n)                | octet varying     | Binary |       |
| ARRAY                       | array             | List | (*3) (*4) |
| ROW                         | record            | TBD | (*4)  |

(*0) Arrowのデータ型はlogical Type([LT])で表される。Logical typeにはパラメーターを持つものもある。パラメーターの詳細は[SC]を参照

(*1) ローカルタイムとして日時を記録する

(*2) UTCとして日時を記録する (タイムゾーンオフセット情報は含められない)

(*3) 要素型は、上記表に含まれるいずれかのデータ型でなければならない

(*4) 2023/12 現在、詳細は検討中

(*5) Arrowにはtimezone-awareなTime型がないため通常のTime型を使用する。TIMEとTIME WITH TIME ZONEのダンプ結果は同じ型を使用するため、ArrowのメタデータだけからローカルタイムなのかUTCなのかを区別する手段はない

(*6) オプションの指定(ArrowFileFormat::character_field_type)により`FixedSizeBinary byteWidth:n`とすることが可能(PG-Strom連携用)。この場合`BINARY(n)`列のダンプ結果と同じ型となるため、Arrowのメタデータだけから `CHAR(n)`/`BINARY(n)`の区別をする手段はない

### ヌルの表現

- ファイル内のレコードの全ての列はヌルを格納することができる
- ヌルの表現方法もArrowの仕様に準じる([VB])

### 列名

Arrowファイルフォーマットに従い、列名はファイルにメタデータとして保存される。
  - ダンプはクエリの出力の列名をここに保存する
  - ロードはこの列名と同名のホスト変数を利用し、ファイル内の列の値をSQL文で利用することができる

### 圧縮方式・エンコーディング

Arrowファイルフォーマットは複数の圧縮方式([CO])をサポートする。当文書ではこれらについては規定しない。
- 正しいArrowファイルフォーマットで保存されている限りは任意の圧縮方式を使用してよい

### ダンプファイル名

Parquetを利用した場合([PD])と同じ、ただし、拡張子は`.arrow`とする

## ダンプ実行結果セットフォーマット

Parquetを利用した場合([PD])と同じ

## Arrow 前提バージョン

本提書の記述はApache Arrow 14.0.2-1を前提としている
https://arrow.apache.org/release/14.0.2.html

ダウンロード・インストール方法などは下記を参照
https://arrow.apache.org/install/

## リファレンス

[PD] [FF] [LT] [SC] [VB] [CO]

[PD]: https://github.com/project-tsurugi/jogasaki/blob/master/docs/dump-file-format-ja.md
[FF]: https://arrow.apache.org/docs/format/Columnar.html#
[LT]: https://arrow.apache.org/docs/format/Columnar.html#logical-types
[SC]: https://github.com/apache/arrow/blob/main/format/Schema.fbs
[VB]: https://arrow.apache.org/docs/format/Columnar.html#validity-bitmaps
[CO]: https://arrow.apache.org/docs/cpp/api/ipc.html#_CPPv4N5arrow3ipc15IpcWriteOptions5codecE

