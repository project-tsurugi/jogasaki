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
using SequenceId = std::size_t;
using SequenceValue = std::int64_t;
using SequenceValueVersion = std::size_t;

extern "C" StatusCode sequence_put(
    TransactionHandle transaction,
    SequenceId id,
    SequenceValue value,
    SequenceValueVersion version);

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


