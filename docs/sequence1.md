# Sequence

2021-02-08 kurosawa

## この文書について

実行エンジン上で、主キーの存在しない表を扱う場合、ユニークな行IDを生成する必要がある。その際に使用するシーケンス生成器の設計について記述する。

## シーケンスの要件

* シーケンスは必要な個数作成する事ができる
* 作成されたシーケンスsから非負64ビット整数を取得する事ができる
  * 取得回数が64ビット整数の最大値を越えた場合の挙動は未定義
* シーケンスsの戻す値はsの生存期間において毎回異なる
* シーケンスsの値v1を観測後にsから取得された値v2はv1より大きいことが望ましい
* シーケンスの生存期間はDBを再起動しても連続している
  * ただしシーケンスsから値を取得後、durable になる前にdbが停止した場合、前回に durable になってから停止までの間に行われたsへの操作について、上記の要件を無視できる

## 設計方針

* 実行エンジン内部のシーケンス生成器がシーケンスをインメモリに保持し、必要に応じてインクリメントして値を決定して、使用側に提供する
  * atomicカウンターなどで複数スレッドからの使用を保護する

* トランザクションエンジンはシーケンス値をトランザクションに関連付けてdurableに保存する機能を提供
  * 各シーケンスsに対してトランザクションを指定してsの現在値を登録可能
  * 登録に際して指定されたトランザクションがdurableになった時点で、登録されたsの値もdurableに保存される事を保証
  * sに対して登録されdurableになった値のうちから最大のものを取得する関数を提供する

## sharksfin API拡張

```
using SequenceId = std::size_t;
using SequenceValue = std::size_t;

extern "C" StatusCode sequence_register(
    TransactionHandle transaction,
    SequenceId id,
    SequenceValue value);

extern "C" StatusCode sequence_get(
    DatabaseHandle handle,
    SequenceId id,
    SequenceValue* value);

extern "C" StatusCode sequence_delete(
    DatabaseHandle handle,
    SequenceId id);
```

## その他考慮事項

* 実行エンジンはインデックスを構成するシーケンスに関する情報(sequence id)などをメタ情報として記録しておく必要がある。
  * 既存のcontent_put/content_getを利用してトランザクションエンジンにシステム管理用の特殊なレコードとして保存する
  * この際のDBアクセスはCCによる排他制御のスコープ外とする
    * 主にリカバリやDDLの処理の一部


