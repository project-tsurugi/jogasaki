# SQL実行エンジンにおけるトランザクションの状態管理

## 本文書について

- 実行エンジンはトランザクションの状態や生存区間を管理する。(`jogasaki::transaction_context`オブジェクト) 
  - CCエンジンにおけるトランザクション状態に依存するが、そのものではなく完全には一致しない
- 本文書は実行エンジン視点でのトランザクション状態とその遷移について記述する

## トランザクション状態一覧

* init
  * トランザクションの開始前から、トランザクションハンドルを利用者に返すまでの状態 (初期状態)
* active
  * トランザクションのユーザ操作を受け付けている状態
* going-to-commit 
  * トランザクションのコミット要求を受けコミット処理を開始したが、まだCCへは要求していない状態
* cc_committing
  * CCへコミット要求を行い、まだ完了していない状態
* committed
  * トランザクションのコミットに成功した状態 (終了状態)
* going-to-abort
  * トランザクションのアボート要求を受けアボート処理を開始したが、まだCCへは要求していない状態
* aborted
  * トランザクションがアボートした状態 (終了状態)

## 操作一覧

* ready
  * トランザクションが開始され、トランザクションハンドルが利用可能になった
* r/w
  * トランザクション内の読み書き操作を行った(成功または失敗)
* cancel
  * ジョブのキャンセルがリクエストされ、それを受理した
* commit
  * コミットがリクエストされ、(jogasakiでの) コミット処理を開始した
* cc_commit
  * コミットが CC へリクエストされ CC での処理を開始した
* task-empty
  * トランザクションを使用しているタスクの同時実行数が0になった
* abort
  * アボートがリクエストされ、それを受理した
* accept
  * CC がコミットを完了させた
* reject
  * CC がコミットを拒否した

## 状態遷移マトリックス

| 状態 \ 操作         | ready  | r/w                 | abort, cancel [^6]         | commit                              | cc_commit  | accept    | reject  | task-empty | 公開される状態名 [^7] |
|-----------------|--------|---------------------|----------------------------|-------------------------------------|------------|-----------|---------|------------|----------------------------| 
| init            | active | -                   | aborted                    | -                                   | -          | -         | -       | -          | -                          |
| active          | -      | active/aborted [^4] | aborted/going-to-abort[^1] | going-to-commit/going-to-abort [^5] | -          |           | -       | -          | RUNNING                |
| going-to-commit | -      | - [^2]              | - [^3]                     | - [^2]                              | cc_committing | -         | -       | -          | COMMITTING             |
| going-to-abort  | -      | - [^2]              | - [^3]                     | - [^2]                              | -          | -         | -       | aborted    | ABORTING               |
| cc_committing      | -      | - [^2]              | - [^3]                     | - [^2]                              | -          | committed | aborted | -          | COMMITTING             |
| aborted         | -      | - [^2]              | - [^3]                     | - [^2]                              | -          | -         | -       | -          | ABORTED                |
| committed       | -      | - [^2]              | - [^3]                     | - [^2]                              | -          | -         | -       | -          | AVAILABLE/STORED       |

[^1]: 実行中のタスクがあれば `going-to-abort` へ遷移、そうでなければ `aborted` へ遷移

[^2]: inactive transaction エラーとして通知

[^3]: abort, cancelはエラー通知しない

[^4]: 操作が成功であれば `active` へ遷移、失敗であれば `aborted` へ遷移

[^5]: 実行中のタスクがあれば `going-to-abort` へ遷移、そうでなければ `going-to-commit` へ遷移

[^6]: cancel と abort はトランザクションの状態遷移上は共通の処理

[^7]: 公開される状態名については tsurugi-issues #346 を参照

## 状態遷移図

[状態遷移マトリックス](#状態遷移マトリックス) の主な部分を下記に示す

```mermaid
graph LR
  init -- ready --> active
  init -- "abort, cancel" --> aborted
  active -- r/w (成功) --> active
  active -- r/w (失敗) --> aborted
  active -- "commit (同時実行タスクなし)" --> going-to-commit
  active -- "commit (同時実行タスクあり)" --> going-to-abort
  active -- "abort, cancel (同時実行タスクなし)" --> aborted
  active -- "abort, cancel (同時実行タスクあり)" --> going-to-abort
  going-to-commit -- cc_commit --> cc_committing
  cc_committing -- accept --> committed
  cc_committing -- reject --> aborted
  going-to-abort -- "task-empty" --> aborted
```