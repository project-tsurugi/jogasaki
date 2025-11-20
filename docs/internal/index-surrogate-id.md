# プライマリ・セカンダリインデックスのサロゲートIDの導入

2025-11-19 kurosawa

## 本文書について

本文書は本リリース(1.8)で導入予定のインデックスのサロゲートIDに関する設計を記述する

## 用語

- サロゲートID(index surrogate ID): 本リリース以降で新規作成されるインデックスに対して導入される、インデックスの実体を一意に識別するために使用する整数(符号無64ビット)。これまではインデックス名がキーであるようにしていたが、インデックス名は変更されるため不変なサロゲートIDを導入する
- ストレージキー(storage key): sharksfin/shirakami のAPI上で、ストレージを一意に特定するために使用するバイナリ列
- ストレージID(storage ID): shirakamiが用いる、ストレージを一意に識別する符号無64ビット整数 ( `shirakami::Storage` ) 
  - limestoneにも渡されて永続化される

## 背景

- jogasakiのテーブルは1個のプライマリインデックス、0個以上のセカンダリインデックスで構成される
- プライマリインデックスはテーブルと同一の名前を持ち、セカンダリインデックスはCREATE INDEX文で指定された名前を持つ
- インデックスはプライマリ・セカンダリいずれもsharksfin/shirakamiのストレージにマッピングされる
- ストレージの特定にはストレージキーが使用される
- ストレージキーには任意のバイナリ列を使用可能だが、*既存のjogasakiの実装ではインデックス名を使用していた*
- このためインデックス名(テーブル名でもあることも多い)とストレージの対応は1:1に固定されてしまい、下記のような問題により新規機能の実装が困難になっている
  - ALTER TABLEでSQL上のテーブル名が変更されると、テーブル名・インデックス名とストレージキーが乖離してしまう
  - TRUNCATE TABLEの実装にあたって、テーブル名を維持しつつ背後のストレージを再作成するという方式を実施したいが、テーブル名とストレージキーが1:1のために対応できない
  - DROP TABLEにおいて、テーブルとストレージの削除のタイミングを分離し、ストレージ削除を遅延させたいという要求がある。しかしDROP TABLE 直後に同名でCREATE TABLEが実行されるとストレージキーが衝突するためCREATE TABLEが失敗してしまう
- これらの問題を解決するために、インデックス名とは別のサロゲートIDを新規作成のインデックスに割当て、それをストレージキーとして使用する
  - 既存のインデックスは引き続き作成時のインデックス名をストレージキーとして使用することで後方互換性を維持する

## 設計方針

- jogasakiにインデックスのサロゲートID(index surrogate ID)の概念を導入する
  - サロゲートIDは本リリース以降で作成される新規インデックスに対して一意に割り当てられる符号無64ビット整数
- jogasakiがsharksfin/shirakami APIを呼び出す際のストレージキーを下記のように変更する
  - サロゲートIDを持つ場合は、そのサロゲートIDをbig-endianのバイナリ列に変換したものをストレージキーとして使用する
  - サロゲートIDを持たない場合(既存のインデックス)は、従来通り(作成時の)インデックス名をストレージキーとして使用する

- 必要に応じてshirakami/sharksfin APIに取得関数を追加する
  - shirakami APIにストレージIDからストレージキーを取得するためのAPI
  - sharksfin APIにStorageHandleからストレージIDやストレージキーを取得するためのAPI
  - ただしこれらは必須ではない可能性があるので、実装時に判断する

## jogasakiのメタデータ

jogasakiはインデックスのメタデータを `storage.proto` の `IndexDefinition` 定義に基づいてシリアライズし、ストレージオプションとしてsharksfin/shirakamiのストレージに保存して永続化している。
インメモリではこの内容を `yugawara::storage::basic_configurable_provider` クラスのような形で保持しており、永続化メタデータに対する更新は適切なタイミングでインメモリ上のデータ構造に反映する必要があるが、本文書では永続化メタデータを主に扱い、インメモリの詳細はここでは省略する。

### jogasakiのメタデータの変更点

- `storage.proto` の `IndexDefinition` メッセージにストレージキー用のフィールド `storage_key` を下記のように追加する

  ```
  // the definition of indices.
  message IndexDefinition {
      ...
      ...

      // the optional storage key.
      oneof storage_key_optional {
          // the index storage key.
          bytes storage_key = 25;
      }

      ...
      ...
  }
  ```

  - 1.8以降のリリースで `IndexDefinition` が新規に作成または更新される際にはこのフィールドにサロゲートIDをバイト列として保存する
  - 既存のインデックスにはこのフィールドは存在しない。jogasakiは既存の名前( `IndexDefinition.name.element_name` フィールド)を読み取り、それをストレージキーとして使用する
    - 既存のインデックスに対して、このフィールドを追加するマイグレーションは自動的には行わない
    - ただし、既存のメタデータが更新される場合、更新後のメタデータはこのフィールドを含む
      - 例えばテーブルがALTER TABLE等によって変更される場合、プライマリインデックスにこのフィールドが追加される

## sharksfinの変更点 (オプション)

`StorageHandle` からストレージキーやストレージIDを取得するためのAPI関数 `sharksfin::storage_key()` と `sharksfin::storage_id()` を追加する。(ストレージIDはオプショナルだが、 `StorageHandle` よりも持ち回りやすいため利便性のために利用できるようにする)

```
/**
 * @brief get the storage key for the given storage handle.
 * @param[in] handle The storage handle.
 * @param[out] result The key associated with the storage handle. It is available only if StatusCode::OK was returned.
 * @return StatusCode::OK if the target storage key was successfully retrieved
 * @return otherwise if error was occurred
 */
StatusCode storage_key(
    StorageHandle handle,
    Slice& result);

/**
 * @brief get the storage id for the given storage handle.
 * The storage id is implementation defined value to uniquely identify the storage.
 * @param[in] handle The storage handle.
 * @param[out] result The storage id for the storage handle. It is available only if StatusCode::OK was returned.
 * @return StatusCode::OK if the target id was successfully retrieved
 * @return otherwise if error was occurred
 */
StatusCode storage_id(
    StorageHandle handle,
    std::uint64_t& result);

```

## shirakamiの変更点 (オプション)

また、`shirakami::Storage` からストレージキーを取得するためのAPI関数 `shirakami::get_storage_key()` を追加する。

```
/**
 * @brief get the storage key for the given storage handle.
 * @param[in] storage The storage handle.
 * @param[out] key The key associated with the storage handle.
 * @return Status::OK success.
 * @return Status::WARN_NOT_FOUND not found.
 */
Status get_storage_key(Storage storage, std::string& key);
```

## 付録

### 新規機能対応の概要

本設計にもとづいて、新規機能の対応方針の概要を示す。本設計が有効であることを確認するための紹介であり概略にとどめる。

#### ALTER

- ALTER TABLE/ALTER INDEXによるテーブル名やインデックス名の変更時には名前(`IndexDefinition.name.element_name`フィールド)のみを更新し、 `storage_key` フィールドは変更しない
  - ただし `storage_key` フィールドが存在しない既存のインデックスに対してALTERが実行された場合には、 `storage_key` フィールドを追加し既存の名前をそこにコピーする
- `storage.proto` 内ではインデックスのテーブルへの依存関係が名前ベース (`StorageName`) で記録されているので、それらも変更する。この処理ではストレージキーは関与せず、そのまま維持される。

#### TRUNCATE TABLE

- TRUNCATE TABLEは下記のようにストレージの再作成を行うことで、テーブルを構成するプライマリ・セカンダリインデックスを初期化する
  - sharksfin/shirakamiのストレージ作成APIを使用して新規ストレージを作成し、そのストレージキーを取得する
  - プライマリインデックスの `storage_key` フィールドを書き換えて(存在しない場合は追加)、新規に作成したストレージキーに更新する
  - セカンダリインデックスについても同様にストレージを新規に作成し `storage_key` フィールドを書き換える
- 既存のストレージの削除は遅延させ、適当なタイミングで実施する (下記DROP TABLEと同様)

#### DROP TABLEの遅延ストレージ削除

- DROP TABLE実行時にはjogasakiのメタデータからテーブル・インデックス情報を削除するが、対応するsharksfin/shirakamiのストレージは即座には削除しないようにする方針である
- 本設計により、 DROP TABLE後にすぐ同名でCREATE TABLEが実行された場合も、ストレージキーは新規に生成された一意なサロゲートIDを使用するため衝突しない
- TRUNCATE TABLEはDROP TABLE + CREATE TABLEの処理とほとんど同じになる。違いはDROP TABLEはセカンダリインデックスをカスケードで削除してしまうので CREATE INDEXを別途行う必要があるが、TRUNCATE TABLEはセカンダリインデックスの定義を維持する
