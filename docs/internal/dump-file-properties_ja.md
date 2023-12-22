# options of individual dump file formats

2023-12-14 arakawa (NT)
2023-12-21 kurosawa (NT)

## 本文書について

Tsurugiのダンプオプションを拡張するにあたり、ファイル出力用ライブラリで設定可能な項目を調査した内容を共有するもの

## Apache Parquet

### Parquet options from API


* 参考
  * https://arrow.apache.org/docs/cpp/api/formats.html#_CPPv4N7parquet16WriterPropertiesE

(以下Arrow 14をベースに記載)

名前 | 型 | 対象 | 既定値 | 概要
-----|----|------|-------|------
`version` | string | file | `"2.6"` (Arrow 9では`"2.4"`) | ファイル形式のバージョン
`data_page_version` | string | file | `"V1"` | ページのバージョン (*1)
`created_by` | string | file | 未調査 | 不明
`write_batch_size` | sint64 | file | `1024` | ページに列の値を書き込む際にバッチに分割して実行される。そのバッチのサイズ(列データの個数) (*2)
`enable_dictionary` | bool | column | `true` | 辞書を作成するか否か
`max_row_group_length` | sint64 | file | 1Mi | row group を構成する最大行数
`data_pagesize` | sint64 | file | 1MB | ページサイズの目標上限 (*2)
`encoding` | string | column | `PLAIN` | 列のエンコーディング形式
`compression` | string | column | `SNAPPY` (*3) | 列の圧縮形式
`compression_level` | sint32 | column | 圧縮形式に依存 | 列の圧縮レベル
`codec_options` | struct | column | 圧縮形式に依存 | 列の圧縮形式のオプション
`enable_statistics` | bool | column | `true` | 列の最大・最小値を保持するかどうか
`max_statistics_size` | uint64 | column | 4KB | 列の最大・最小値情報の最大サイズ
`encryption` | struct | file | 暗号化なし | ファイルの暗号化オプション
`sorting_columns` | 列のリスト | file | 空 | ファイルが特定の列でソートされている場合にその列を指定
`enable_store_decimal_as_integer` | bool | file | `false` | precisionの小さいDecimal型を整数で格納するか
`enable_write_page_index` | bool | column | `false` | pageの統計情報(最大・最小値)をpage indexとして一箇所にまとめるか

以下は各値の選択肢。

* `version`
  * `1.0`
  * `2.0` (deprecated - use `2.4` or `2.6` (*4)) 
  * `2.4`
  * `2.6`
* `data_page_version`
  * `V1`
  * `V2`
* `encoding`
  * `PLAIN`
  * `PLAIN_DICTIONARY`
  * `RLE`
  * `BIT_PACKED`
  * `DELTA_BINARY_PACKED`
  * `DELTA_LENGTH_BYTE_ARRAY`
  * `DELTA_BYTE_ARRAY`
  * `RLE_DICTIONARY`
  * `BYTE_STREAM_SPLIT`
* `compression`
  * `UNCOMPRESSED`
  * `SNAPPY`
  * `GZIP`
  * `LZO`
  * `BROTLI`
  * `BZ2`
  * `LZ4`
  * `LZ4_FRAME`
  * `LZ4_HADOOP`
  * `ZSTD`

* *1 - ページ(column chunkの分割)の直列化の構造を定めるもの。V2の取り扱いに以前問題があった(2020年までのライブラリでは正しく扱えていなかった)ので注意
( https://github.com/apache/arrow/blob/3c66491846a24f17014b31a22fafdda0229f881a/cpp/src/parquet/properties.h#L48 )
* *2 - 詳しい説明がないが、ソースコード( https://github.com/apache/arrow/blob/3c66491846a24f17014b31a22fafdda0229f881a/cpp/src/parquet/column_writer.cc#L1222 周辺)によると、下記のような動作をする。
  * arrowが保持する列データから `write_batch_size` 個を取り出してバッチとし、その単位でparquetへ書き込みを行う
  * 書き込んだ結果、ページのサイズが `data_pagesize` (バイト数)を超える場合は、新しいページを追加し次のバッチからはそこに書く

  `data_pagesize`単体では指定されたサイズを大きく超えるケースがあるためバッチへ分割するようにしたそう。
pyarrowのwrite_batch_sizeの説明 ( https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetWriter.html ) も参照。
* *3 - `compression` の既定値は jogasaki 側で設定している (ライブラリの既定値は `UNCOMPRESSED`)
* *4 - 参照 https://github.com/apache/arrow/blob/3c66491846a24f17014b31a22fafdda0229f881a/cpp/src/parquet/type_fwd.h#L44

### Parquet メッセージ案

フィールドの番号は省略

```proto
// dump file format for Apache Parquet.
message ParquetFileFormat {

    // the parquet file format version.
    string parquet_version; // = version

    // the maximum number of rows in the same row group.
    int64 record_batch_size; // = max_row_group_length

    // the approximately maximum row group size in bytes.
    int64 record_batch_in_bytes;

    // common compression codec name of the individual columns.
    string codec; // = compression

    // common encoding type of the individual columns.
    string encoding;

    // settings of each column.
    repeated ParquetColumnFormat columns;
}

// individual columns settings of ParquetFileFormat.
message ParquetColumnFormat {

    // the target column name.
    string name;

    // column compression codec name (overwrites the file format setting).
    string codec;

    // column compression type name (overwrites the file format setting).
    string encoding;
}
```

* 備考
  * 最低限の設定のみ利用可能にする
  * `max_record_batch_size` は直接利用できないようだが、column writer/row group writerから見積もりが取得できる(参考: https://github.com/apache/arrow/blob/3c66491846a24f17014b31a22fafdda0229f881a/cpp/examples/parquet/low_level_api/reader_writer2.cc#L88 )

## Apache Arrow

### Arrow options from API

* 参考
  * [Arrow IPC - struct IpcWriteOptions](https://arrow.apache.org/docs/cpp/api/ipc.html#_CPPv4N5arrow3ipc15IpcWriteOptionsE)

名前 | 型 | 既定値 | 概要
-----|----|-------|------
`allow_64bit` | bool | `false` | 2^31 バイト以上のフィールド値を利用するか
`alignment` | int32 | `8` | アライメントサイズ
`codec` | - | 無圧縮 | 圧縮コーデック
`min_space_savings` | double | 設定なし | 圧縮データを採用する際の最低圧縮率
`metadata_version` | string | `5` | メタデータ形式のバージョン

* `metadata_version`
  * `1` .. `5`
* `codec`
  * `UNCOMPRESSED`
  * `ZSTD`
  * `LZ4_FRAME`

### Arrow メッセージ案

フィールドの番号は省略

```proto
// dump file format for Apache Arrow.
message ArrowFileFormat {

    // the metadata format version.
    string metadata_version;

    // the byte alignment of each values.
    int32 alignment;

    // the maximum number of records in record batch.
    int64 record_batch_size;

    // the approximately maximum size of each record batch in bytes.
    int64 record_batch_in_bytes;

    // compression codec name.
    string codec;

    // threshold for adopting compressed data.
    double min_space_saving;

    // CHAR column metadata type.
    ArrowCharacterFieldType character_field_type;
}

// CHAR column metadata type for Arrow files.
enum ArrowCharacterFieldType {

    // use default metadata type for CHAR columns.
    ARROW_CHARACTER_FIELD_TYPE_UNSPECIFIED,

    // use StringBuilder for CHAR columns.
    STRING;

    // use FixedSizeBinaryBuilder for CHAR columns.
    FIXED_SIZE_BINARY;
}
```

* 備考
  * `codec` の細かな設定は行わず、それらの既定値とする
  * `max_record_batch_size` は直接利用できるか不明なので、利用できないようならテーブルスキーマから割り戻して `record_batch_size` のほうに制限を掛ける
  * 以下のように計算することを想定
    * `record_batch_size` = _A_, `max_record_batch_bytes` = _B_
      * record batch -> `Min(A, B / 推定レコード平均サイズ)`
    * `record_batch_size` = _A_, `max_record_batch_bytes` = `0`
      * record batch -> `A`
    * `record_batch_size` = `0`, `max_record_batch_bytes` = _B_
      * record batch -> `B / 推定レコード平均サイズ`
    * `record_batch_size` = `0`, `max_record_batch_bytes` = `0`
      * record batch -> デフォルト値 (?)


## 付録(バックアップ)

### Parquet options from API (pre-Arrow version)

* 参考
  * https://github.com/apache/parquet-cpp/blob/642da055adf009652689b20e68a198cffb857651/src/parquet/properties.h#L141
* 備考
  * Arrow 版でないAPIを利用しており、情報が少ない
  * Arrow 版と指定できる内容が異なる

名前 | 型 | 対象 | 既定値 | 概要
-----|----|------|-------|------
`version` | string | file | `"1.0"`` | ファイル形式のバージョン
`created_by` | string | file | 未調査 | 不明
`write_batch_size` | sint64 | file | `1024` | row group を構成する最大行数 (*1)
`enable_dictionary` | bool | column | `true` | 辞書を作成するか否か
`max_row_group_length` | sint64 | file | 64MB | row group の最大バイト数 (*2)
`data_pagesize` | sint64 | file | 1MB | ページサイズ (*3)
`encoding` | string | column | `PLAIN` | 列のエンコーディング形式
`compression` | string | column | `SNAPPY` (*4) | 列の圧縮形式
`enable_statistics` | bool | column | `true` | 列の最大・最小値を保持するかどうか
`max_statistics_size` | uint64 | column | 4KB | 列の最大・最小値情報の最大サイズ

* *1 - 詳しい説明がなかった。行数であることは既定値からの推測
* *2 - 詳しい説明がなかった。arrow style では既定値が 1Mi rows となっていて、単位がそもそも違う可能性もある
* *3 - 詳しい説明がなかった。バッファ？
* *4 - `compression` の既定値は jogasaki 側で設定している

以下は各値の選択肢。

* `version`
  * `1.0`
  * `2.0`
* `encoding`
  * `PLAIN`
  * `PLAIN_DICTIONARY`
  * `RLE`
  * `BIT_PACKED`
  * `DELTA_BINARY_PACKED`
  * `DELTA_LENGTH_BYTE_ARRAY`
  * `DELTA_BYTE_ARRAY`
  * `RLE_DICTIONARY`
* `compression`
  * `UNCOMPRESSED`
  * `SNAPPY`
  * `GZIP`
  * `LZO`
  * `BROTLI`
  * `LZ4`
  * `ZSTD`
