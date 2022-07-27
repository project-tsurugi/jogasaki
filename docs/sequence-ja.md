# Sequence

2021-02-08 kurosawa
2021-03-09 kurosawa (最小・最大等の要件を追加)

## この文書について

実行エンジン上で、主キーの存在しない表を扱う場合、ユニークな行IDを生成する必要がある。その際に使用するシーケンス生成器の設計について記述する。

## シーケンスの要件

* シーケンスは必要な個数作成する事ができる
* 作成されたシーケンスsから符号付き64ビット整数を取得する事ができる
  * 取得回数が符号無64ビット整数の最大値を越えた場合の挙動は未定義
* 定義時に下記のパラメータを設定できる(将来的にはCREATE SEQUENCE文による作成を想定)
  * 初期値
  * 最小値
  * 最大値
  * インクリメント幅
  * サイクル(最小値や最大値を越えた際に最大値、最小値に戻る機能)の有無
* シーケンスsの戻す値はsの生存期間において毎回異なる
  * ただし最大値か最小値が設定されており、そのレンジを越えた場合は例外
* シーケンスs(インクリメント幅inc)の値v1を観測後、再度sから取得された値v2に対し、レンジを越えない場合は次が成り立つ
  * v2 >= v1 + inc (incが正の場合)
  * v2 <= v1 + inc (incが負の場合)である
* シーケンスの生存期間はDBを再起動しても連続している
  * ただしシーケンスsから値を取得後、durable になる前にdbが停止した場合、前回に durable になってから停止までの間に行われたsへの操作について、上記の要件を無視できる

## 設計方針

* 実行エンジン内部のシーケンス生成器がシーケンスをインメモリに保持し、必要に応じてインクリメント/デクリメントして値を決定し、使用側に提供する
  * atomicカウンターなどで複数スレッドからの使用を保護する
  * 更新ごとに単調に増加するバージョン番号(符号無64ビット整数)をアサインする
* トランザクションエンジンはシーケンス値をトランザクションに関連付けてdurableに保存する機能を提供
  * 各シーケンスsに対してトランザクションを指定してsの現在値とバージョン番号を登録可能
    * シーケンスsに対して同じバージョンが複数回登録されることはない
  * 登録に際して指定されたトランザクションがdurableになった時点で、登録されたsの値もdurableに保存される事を保証
  * sに対して登録されdurableになった値のうち、バージョンが最大のものを取得する関数を提供する

## sharksfin API拡張

```
/**
 * @brief sequence id
 * @details the identifier that uniquely identifies the sequence in the database
 */
using SequenceId = std::size_t;

/**
 * @brief sequence value
 * @details the value of the sequence. Each value in the sequence is associated with some version number.
 */
using SequenceValue = std::int64_t;

/**
 * @brief sequence version
 * @details the version number of the sequence that begins at 0 and increases monotonically.
 * For each version in the sequence, there is the associated value with it.
 */
using SequenceVersion = std::size_t;

/**
 * @brief create new sequence
 * @param handle the database handle where the sequence is created.
 * @param [out] id the newly assigned sequence id, that is valid only when this function is successful with StatusCode::OK.
 * @return StatusCode::OK if the creation was successful
 * @return otherwise if any error occurs
 * @note This function is not intended to be called concurrently with running transactions.
 * Typical usage is in DDL to register sequence objects.
 */
extern "C" StatusCode sequence_create(
    DatabaseHandle handle,
    SequenceId* id);

/**
 * @brief put sequence value and version
 * @details request the transaction engine to make the sequence value for the specified version durable together
 * with the associated transaction.
 * @param transaction the handle of the transaction associated with the sequence value and version
 * @param id the sequence id whose value/version will be put
 * @param value the new sequence value
 * @param version the version of the sequence value
 * @return StatusCode::OK if the put operation is successful
 * @return otherwise if any error occurs
 * @warning multiple put calls to a sequence with same version number cause undefined behavior.
 */
extern "C" StatusCode sequence_put(
    TransactionHandle transaction,
    SequenceId id,
    SequenceValue value,
    SequenceVersion version);

/**
 * @brief get sequence value
 * @details retrieve sequence value of the "latest" version from the transaction engine.
 * Transaction engine determines the latest version by finding maximum version number of
 * the sequence from the transactions that are durable at the time this function call is made.
 * It's up to transaction engine when to make transactions durable, so there can be delay of indeterminate length
 * before put operations become visible to this function. As for concurrent put operations, it's only guaranteed that
 * the version number retrieved by this function is equal or greater than the one that is previously retrieved.
 * @param handle the database handle where the sequence exists
 * @param id the sequence id whose value/version are to be retrieved
 * @param [out] value the sequence value, that is valid only when this function is successful with StatusCode::OK.
 * @param [out] version the sequence's latest version number, that is valid only when this function is successful with StatusCode::OK.
 * @return StatusCode::OK if the retrieval is successful
 * @return otherwise if any error occurs
 * @note This function is not intended to be called concurrently with running transactions.
 * Typical usage is to retrieve sequence initial value at the time of database recovery.
 */
extern "C" StatusCode sequence_get(
    DatabaseHandle handle,
    SequenceId id,
    SequenceValue* value,
    SequenceVersion* version);

/**
 * @brief delete the sequence
 * @param handle the database handle where the sequence exists
 * @param id the sequence id that will be deleted
 * @return StatusCode::OK if the deletion was successful
 * @return otherwise if any error occurs
 * @note This function is not intended to be called concurrently with running transactions.
 * Typical usage is in DDL to unregister sequence objects.
 */
extern "C" StatusCode sequence_delete(
    DatabaseHandle handle,
    SequenceId id);
```

## その他考慮事項

* 実行エンジンはインデックスを構成するシーケンスに関する情報(sequence id)などをメタ情報として記録しておく必要がある。
  * 既存のcontent_put/content_getを利用してトランザクションエンジンにシステム管理用の特殊なレコードとして保存する
  * この際のDBアクセスはCCによる排他制御のスコープ外とする
    * 主にリカバリやDDLの処理の一部


