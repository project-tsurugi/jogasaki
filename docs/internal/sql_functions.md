# SQL関数登録および呼出し実行の内部設計

2025-07-18 kurosawa

## 本文書について

本文書は、SQL実行エンジンにおけるSQL関数の登録および呼出し・実行に関する機能の内部設計について記述する

## 関数の分類

現在サポートされるSQL関数は下記のいずれかに分類される

- スカラ関数
- 集約関数
  - 通常型 (非インクリメンタル)
  - インクリメンタル型

現行ではすべてのSQL関数はビルトインで、ユーザー定義関数のサポートはこれからである

## 構成要素

### function id

- 関数を一意(同一カテゴリ内)に識別する整数
- 実行時に関数本体を索引するキー
- 同名関数でも引数の型が異なる場合は別のidをアサインする
- 一度割り当てたものは不変であり、削除後も再利用は不可

### function provider

SQLコンパイラに対して関数定義(宣言部分)を共有し伝えるためのもの

- スカラ関数: `yugawara::function::configurable_provider` クラス
グローバル関数 `global::scalar_function_provider()` でアクセス可能

- 集約関数: `yugawara::aggregate::configurable_provider` クラス
`api::impl::database::aggregate_functions()` でアクセス可能

これらのproviderに格納されたエントリが `definition_id` としてfunction IDを保持する

### function repository

SQL実行エンジンが関数定義(実行内容や本体)を保持し検索して呼出して使うための情報の格納領域

- function idをキーとして関数詳細情報であるfunction info.を格納するもの
  - 関数種別、引数・戻り値に関する情報など

- スカラ関数: `executor::function::scalar_function_repository`クラス
グローバル関数 `global::scalar_function_repository()` でアクセス可能

- 集約関数: `executor::function::aggregate_function_repository`クラス
グローバル関数 `global::aggregate_function_repository()` でアクセス可能

## スカラー関数の実行

- `expr::evaluator`が `takatori::scalar::function_call` を受け取り、式評価の一部として関数呼出を行う
- 式評価時に `yugawara::function::declaration::definition_id()` によってfunction id を取得し、レポジトリから関数本体を検索
- 渡されたレコードから引数を作成し、関数本体に渡して呼出
- 引数や戻り値は `data::any` を使うので比較的シンプルなインターフェース

## 集約関数

### 通常型(非インクリメンタル型)

- オペレーター `executor::process::impl::ops::aggregate_group` クラス内で実行される
- 1レコードずつこのオペレーターに渡され、一緒に渡される `bool last_member` フラグによりグループの最終レコードを判定
- 最終レコードが渡されるまで、グループのレコードを貯めて、最後に集約計算を行う

### インクリメンタル型

- 通常型の処理を最適化するもの
  - エクスチェンジによって集約処理を並列化する
  - グループのメンバーレコードがすべて揃わなくても可能な計算を先に実行し、その後にグループ全部を集約、最終処理を行う
  - 必ずしもすべての集約関数がインクリメンタル型で実装できるわけではない
- aggregateエクスチェンジによって実行される(`executor::exchange::aggregate`)
- エクスチェンジが行うべき集約処理の全情報は `aggregate_info` オブジェクトで管理
- 集約処理を下記 3フェーズの関数実行に分割:
  - **pre**: 上流プロセスが input_partition データ書き込み時に計算
  - **mid**: 中間総合時
  - **post**: 最終出力前

- 例: 平均値の計算関数(AVG)
  - pre: パーティションごとに渡された値のsum, countを計算
  - mid: パーティションの sum/count を合算
  - post: sum を count で割り平均を算出

- SUM などは post 処理は不要でpre/midのみ

## 関数登録と拡張方法

### 登録フロー

- 現状はビルトイン関数のみなのでdatabase::init() で一括登録
  - repositoryとprovider両方へ同時に登録
- カテゴリ別(スカラ関数・集約関数)に異なるレポジトリに登録

### function idの割当

- 登録主体ごとのID範囲分割
  - 10000〜19999: ビルトイン (参照：`yugawara::aggregate::declaration::minimum_builtin_function_id`)
  - 20000〜: user defined (参照：`yugawara::aggregate::declaration::minimum_user_function_id`)
- DDLにより永続化される情報となる可能性もあるので変更・再利用禁止

## その他注意

- 本来はすべての集約関数は通常型の実装を持ち、最適化可能な場合にインクリメンタル型も実装すべき
  - 現状はそうなっていないため、インクリメンタル型と通常型を両方を同じSQLで行おうとした場合にエラーになる制限がある (https://github.com/project-tsurugi/tsurugi-issues/issues/959)
