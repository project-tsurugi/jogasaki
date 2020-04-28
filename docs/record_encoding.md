# レコードのバイナリ表現

## この文書について

実行エンジンはリレーショナルデータを主にレコード単位で扱う
この文書はレコードをメモリ上に展開する時のバイナリ表現を規定する

## 用語

* フィールドタイプ
実行エンジンが規定するレコードのフィールドの型
上位(takatori)から与えられた型をもとに、実行エンジンが使用する情報を抽出し必要な情報を付加したもの

* ランタイムタイプ
実際に値を格納するデータ領域に対応するC++の型
フィールドタイプから決まる

## レコードのバイナリ表現仕様

* レコードは高々1個nullityフィールドと1個以上の値フィールドを含む連続領域からなる。値フィールドはレコードにおける列の値を格納するもの、nullityフィールドは列のnullityビットを格納するものである。
* 値フィールドとnullityフィールドのバイナリ表現を規定された順番とアラインメントに従って並べたものがレコードのバイナリ表現である
* 値フィールドは対応するフィールドタイプにランタイムタイプが定められており(下記「タイプごとのバイナリ表現仕様」参照)アラインメントはランタイムタイプから決定する。
* nullityフィールドはレコードのメタ情報で規定されるバイトサイズを持ち、アラインメントは1バイトとする。
* 列に対してnullityフィールドのどのビットが使用されるか(nullityビットオフセット)はレコードメタ情報の一部として規定する

## タイプごとのバイナリ表現仕様

基本方針
  * 各フィールドが使用するランタイムタイプを規定し、OS/CPUアーキテクチャで決まるネイティブなアラインメントとエンディアンによって決まるバイナリ表現を原則使用する。
  * レコードのアラインメントはそのレコードに含まれるフィールドのうち最も大きなアラインメントとする
  * ランタイムタイプは基本的にtakatoriがコンパイル時計算に使用しているものに合わせる。ただしものによってtrivially copyableでなく、実行エンジンの用途に合わないので、適切な型を実行エンジンで作成する(characterの場合のtextなど)

下記にtakatoriの型(takatori/type/type_kind.hを参照)に対して実行エンジンが利用するランタイムタイプを示す。

* boolean
    std::int8_t
* int1
    std::int32_t
* int2
    std::int32_t
* int4
    std::int32_t
* int8
    std::int64_t
* float4
    float
* float8
    double 
* decimal
    fpdecimal::Decimal
    (メモ: alignment 4バイトの模様)
* character
    text
* bit
TBD
* date
    takatori::datetime::date
* time_of_day
    takatori::datetime::time_of_day
* time_point
    takatori::datetime::time_point
* time_interval
TBD

以下の型についてはTBD
* array
* record
* unknown
* row_reference
* row_id
* declared
* extension
