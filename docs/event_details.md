# DAGコントローラーイベント詳細

### イベント
イベントはDAGコントローラの判断で実施される内部イベントとDAGコントローラの外部から発生する外部イベントに分かれる。
内部イベントは動詞で、外部イベントは状態を表す形容詞で示す。

- activate(内部)
    - 開始条件:
        ステップがCREATEDであること
        上流ステップが全てACTIVATEDであること(TBD:データフローオブジェクトが独立してないので不要か)
        (最適化: 上流ステップのconsume直前まで遅らせてもよい)
    - アクション:
        ステップにactivateを指示する(引数なし)
        ステップに副入力がある場合は副タスク用のスロットを作成する
    - 終了後状態
        ステップに副入力がある場合はACTIVATED、そうでなければPREPARED

- prepare(内部)
    - 開始条件:
        ステップが[ACTIVATED, PREPARING]であること
        副入力ポートの一つがcompletedへ遷移
    - アクション:
        ステップに副タスクの作成を指示(引数: 状態が変化した副入力ポート番号)
        タスクスケジューラーにタスクをsubmit
        管理するタスクスロットのステートをrunningへ書き換え
    - 終了後状態
        PREPARING

- preparation completed(外部) - 副タスク完了通知
    - 開始条件:
        ステップに対して起動済みの副タスクの一つが完了
        (必然的にステップはPREPARING以降であることが前提となる)
    - アクション:
        管理する副タスクスロットのステートをcompletedへ書き換える
    - 終了後状態
        副タスクスロットを確認し、副タスクが全て終了している場合はPREPARED、そうでない場合は現在のステートを維持

- consume(内部)
    - 開始条件:以下の全てを満たすこと
        - ステップがPREPAREDであること
        - 下流ステップが存在しないか、存在する下流ステップが全てACTIVATED以降であること
        - 入力ポートを持たないか、全入力ポートがcompletedであること
    - アクション:
        ステップに主タスクの作成を指示(引数なし)
            - Executor側では入力ポートがactiveかcompletedなことから上流からsrc/readerの取得が可能であり、その数に基づいてタスク数を決定する。
        タスクスケジューラーにタスクをsubmit
        管理するタスクスロットのステートをrunningへ書き換える
    - 終了後状態
        RUNNINGへ遷移

- upstream providing(外部) - 上流がデータ送信を開始したのでタスクの作成が必要かもしれないという通知
    - 開始条件:以下の全てを満たすこと
        - 上流ステップのいずれかが当ステップにupstream providingイベントによってデータ送信開始を通知した
        - ステップが[ACTIVATED, PREPARING］または[PREPARED, RUNNING]のいずれかであること。前者は副入力に対する通知で、後者は主入力に対する通知である必要がある。
    - アクション:
        イベントを生成した上流ステップが結びついている当ステップのポートの主/副を確認
        副入力の場合はprepareのアクションと同様
        主入力の場合はconsumeのアクションと同様
    - 終了後状態
        作成したタスクが副タスクの場合はPREPARING
        主タスクの場合はRUNNING

- main completed(外部) - 主タスク完了通知
    - 開始条件:
        ステップに対して起動済みの主タスクの一つが完了
        (必然的にステップはRUNNING以降であることが前提となる)
    - アクション:
        管理する主タスクスロットのステートをcompletedへ書き換える
    - 終了後状態
        主タスクスロットを確認し、主タスクが全て終了かつ主入力ポートが全てcompleted(プロセスの場合はこれはタスクの終了条件から導かれる)である場合はCOMPLETEDへ遷移、そうでない場合は現在のステートを維持
- deactivate(内部)
    - 開始条件:以下の全てを満たすこと
        - 上流ステップが存在しないか、全上流ステップがCOMPLETEDであること
        - 下流ステップが存在しないか、存在して下記のいずれかを満たす
          - 全下流ステップがCOMPLETEDであること
          - 下流ステップが副入力ポートを通して当ステップと接続しており(つまりBroadcastエクスチェンジ)、全下流ステップがPREPARED以降であること
        
    - アクション:
        ステップにdeactivateを指示(引数なし)
    - 終了後状態
        DEACTIVATEDへ遷移

- completion instructed(外部) - ステップ処理の早期完了要求通知
    - 開始条件:
        ステップに対するcompletion instructedイベントを受理
    - アクション:
        タスクスロットから現在実行中の主/副タスクを取得し、タスクスケジューラに早期終了を指示
    - 終了後状態
        COMPLETINGへ遷移 ただし早期終了を指示するタスクがなかった場合にはCOMPLETEDへ遷移

- propagate downstream completing state(内部)
    - 開始条件:
        下流ステップが1個以上存在し、全下流ステップが[COMPLETING, COMPLETED]であること
    - アクション:
        completion instructedイベントを発行
    - 終了後状態
        COMPLETINGへ遷移

## 状態遷移

https://docs.google.com/spreadsheets/d/1mBqYMKNId1uMXnCxot_Xmjz9Vm-8ahC1Dc4qAtsPZK0/edit?usp=sharing

### 例

https://docs.google.com/presentation/d/1x45LtUf1YmCOOV5v2kFaIyejCHaaPzB_AFi72x84CC4/edit#slide=id.g6eb6ab093e_0_259