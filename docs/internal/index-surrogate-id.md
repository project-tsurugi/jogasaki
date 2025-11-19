# プライマリ・セカンダリインデックスのサロゲートIDの導入

2025-11-19 kurosawa

## 本文書について

本文書は次リリース(1.8)で導入予定のインデックスのサロゲートIDに関する設計を記述する

## 用語

- サロゲートID(index surrogate ID): 本設計で導入される、jogasakiがインデックスの実体を一意に識別するために使用するバイナリ列。インデックス名とは独立しており、インデックス名が変更されてもサロゲートIDは変更されない
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
- これらの問題を解決するために、インデックス名ではなく一意なサロゲートIDをストレージキーとして使用する設計を導入する

## 設計方針

- jogasakiにインデックスのサロゲートID(index surrogate ID)の概念を導入する
- sharksfin/shirakami API上でストレージを特定する際はストレージキーとしてサロゲートIDを使用する
- ストレージIDはjogasakiが後方互換性と運用性を考慮して定義するバイナリ列である。具体的には下記のように実装する。
  - 既存のインデックス(1.7以前で作成)の場合は下記のようにインデックス名をそのままサロゲートIDとする(互換性維持のため)。
    - プライマリインデックス: CREATE TABLE実行時のテーブル名
    - セカンダリインデックス: CREATE INDEX実行時のインデックス名
  - 新規インデックス(1.8以降で作成)の場合は sharksfin/shirakamiがストレージに対して付加するストレージキーと同一のバイナリ列をサロゲートIDとする
    - 既存のsharksfin/shirakami APIはストレージ作成時にストレージキーを呼び出し側が指定する方式だったがこれを拡張し、呼び出し側がストレージキーを指定しなくてもよいようにする
    - shirakamiは一意なバイナリ列を生成してストレージキーとして割り当てる
      - shirakamiのストレージIDをbig-endianバイナリ列に変換したものを使用すると、自動的に一意なバイナリ列を生成することができるためコードの変更が最小限で済みそう
    - sharksfin/shirakamiのAPIにStorageHandleやストレージIDからストレージキーを取得するためのAPIを追加する
      - アサインされたストレージキーを受け取るため

## sharksfinの変更点

ストレージキーを受け取らずにストレージを新規作成するAPI関数 `sharksfin::storage_create()` を追加する。ストレージキーが自動的に生成される以外は既存の `sharksfin::storage_create()` と同様に動作する。
また、`StorageHandle` からストレージキーやストレージIDを取得するためのAPI関数 `sharksfin::storage_key()` と `sharksfin::storage_native_handle()` を追加する。(ストレージIDはオプショナルだが、 `StorageHandle` よりも持ち回りやすいため利便性のために利用できるようにする)

```
/**
 * @brief creates a new storage space onto the target database with storage options
 * New storage key is automatically generated and assigned to the created storage.
 * The created handle must be disposed by storage_dispose().
 * @param handle the target database
 * @param options the options to customize storage setting
 * @param result [OUT] the output target of storage handle, and it is available only if StatusCode::OK was returned
 * @return StatusCode::OK if the target storage was successfully created
 * @return otherwise if error was occurred
 */
StatusCode storage_create(
    DatabaseHandle handle,
    StorageOptions const& options,
    StorageHandle *result);

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
 * @brief get the native storage handle value for the given storage handle.
 * The native storage handle is implementation defined value to uniquely identify the storage.
 * @param[in] handle The storage handle.
 * @param[out] result The native storage handle value for the storage handle. It is available only if StatusCode::OK was returned.
 * @return StatusCode::OK if the target native handle was successfully retrieved
 * @return otherwise if error was occurred
 */
StatusCode storage_native_handle(
    StorageHandle handle,
    std::uint64_t& result);

```

## shirakamiの変更点

ストレージキーを受け取らずにストレージを新規作成するAPI関数 `shirakami::create_storage()` を追加する。ストレージキーが自動的に生成される以外は既存の `shirakami::create_storage()` と同様に動作する。また、`shirakami::Storage` からストレージキーを取得するためのAPI関数 `shirakami::get_storage_key()` を追加する。

```
/**
 * @brief Create new storage, and return its handler.
 * New storage key is automatically generated and assigned to the created storage.
 * @param[out] storage The storage handle mapped for the generated key.
 * @param[in] options If you don't use this argument, @a storage is specified
 * by shirakami, otherwise, is specified by user.
 * @return Status::ERR_FATAL_INDEX Some programming error.
 * @return Status::OK if successful.
 * @return ...
 * @return ...
 */
Status create_storage(Storage& storage, storage_option const& options = {});

/**
 * @brief get the storage key for the given storage handle.
 * @param[in] storage The storage handle.
 * @param[out] key The key associated with the storage handle.
 * @return Status::OK success.
 * @return Status::WARN_NOT_FOUND not found.
 */
Status get_storage_key(Storage storage, std::string& key);
```

## jogasakiのメタデータ

jogasakiはインデックスのメタデータを `storage.proto` の `IndexDefinition` 定義に基づいてシリアライズし、ストレージオプションとしてsharksfin/shirakamiのストレージに保存して永続化している。
インメモリではこの内容を `yugawara::storage::basic_configurable_provider` クラスのような形で保持しており、永続化メタデータに対する更新は適切なタイミングでインメモリ上のデータ構造に反映する必要があるが、本文書では永続化メタデータを主に扱い、インメモリの詳細はここでは省略する。

### jogasakiのメタデータの変更点

- `storage.proto` の `IndexDefinition` メッセージにサロゲートID用のフィールド `surrogate_id` を下記のように追加する

  ```
  // the definition of indices.
  message IndexDefinition {
      ...
      ...

      // the optional surrogate identifier.
      oneof surrogate_id_optional {
          // the index surrogate id.
          bytes surrogate_id = 25;
      }

      ...
      ...
  }
  ```

  - 1.8以降のリリースで `IndexDefinition` が新規に作成または更新される際にはこのフィールドにサロゲートIDが保存される
  - 既存のインデックスにはこのフィールドは存在しない。jogasakiは既存の名前( `IndexDefinition.name.element_name` フィールド)を読み取り、それをサロゲートIDとして使用する
    - 既存のインデックスに対して、このフィールドを追加するマイグレーションは自動的には行わない
    - ただし、既存のメタデータが更新される場合、更新後のメタデータはこのフィールドを含む
      - 例えばテーブルがALTER TABLE等によって変更される場合、プライマリインデックスにこのフィールドが追加される

## 注意・考慮事項

- 現在の設計では、新規インデックスに対するサロゲートIDが本質的に `shirakami::Storage` であるということをjogasakiは使っておらず、あくまでshirakami/sharksfinが生成する任意のバイト列としてみている
  - しかし場合によっては既存インデックスと新規インデックスをjogasakiが区別して処理を分岐する必要があるかもしれない。
  - その場合はサロゲートIDが先頭に `0x00` を含むかどうかで区別できると考えている

## 付録

### 新規機能対応の概要

本設計にもとづいて、新規機能の対応方針の概要を示す。本設計が有効であることを確認するための紹介であり概略にとどめる。

#### ALTER

- ALTER TABLE/ALTER INDEXによるテーブル名やインデックス名の変更時には名前(`IndexDefinition.name.element_name`フィールド)のみを更新し、 `surrogate_id` フィールドは変更しない
  - ただし `surrogate_id` フィールドが存在しない既存のインデックスに対してALTERが実行された場合には、 `surrogate_id` フィールドを追加し既存の名前をそこにコピーする
- `storage.proto` 内ではインデックスのテーブルへの依存関係が名前ベース (`StorageName`) で記録されているので、それらも変更する。この処理ではサロゲートIDは関与せず、そのまま維持される。

#### TRUNCATE TABLE

- TRUNCATE TABLEは下記のようにストレージの再作成を行うことで、テーブルを構成するプライマリ・セカンダリインデックスを初期化する
  - sharksfin/shirakamiのストレージ作成APIを使用して新規ストレージを作成し、そのストレージキーを取得する
  - プライマリインデックスの `surrogate_id` フィールドを書き換えて(存在しない場合は追加)、新規に作成したストレージキーに更新する
  - セカンダリインデックスについても同様にストレージを新規に作成し `surrogate_id` フィールドを書き換える
- 既存のストレージの削除は遅延させ、適当なタイミングで実施する (下記DROP TABLEと同様)

#### DROP TABLEの遅延ストレージ削除

- DROP TABLE実行時にはjogasakiのメタデータからテーブル・インデックス情報を削除するが、対応するsharksfin/shirakamiのストレージは即座には削除しないようにする方針である
- 本設計により、 DROP TABLE後にすぐ同名でCREATE TABLEが実行された場合も、ストレージキーはshirakamiが生成した一意なサロゲートIDを使用するため衝突しない
- TRUNCATE TABLEはDROP TABLE + CREATE TABLEの処理とほとんど同じになる。違いはDROP TABLEはセカンダリインデックスをカスケードで削除してしまうので CREATE INDEXを別途行う必要があるが、TRUNCATE TABLEはセカンダリインデックスの定義を維持する
