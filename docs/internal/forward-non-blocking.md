# Forward Exchange ノンブロッキング実行

## はじめに

本文書は、forward exchange を経由したパイプラインにおいて、上流プロセスの完了を待たず、
上流が書き込んだレコードを順次下流プロセスへ送出するノンブロッキング実行の設計を記述する。

## 背景

### forward exchange の役割

forward exchange (`model::step_kind::forward`) は、上流プロセスステップのレコードを
変換なしに下流プロセスステップへ受け渡すためのパッシブなデータバッファとして機能する。

## 現状の実装と課題

- forward exchange を経由したパイプラインの実行順序は完全に直列となっている

## 目的と利点

- パイプライン並列度の向上: forward exchange上流の書き込みと 下流の読み出しを並行動作させる。
- ビジーウェイトの除去: `next_record()` が false を返した場合に yield して、レコードがない間はスレッドを解放する。
- レイテンシ削減: P1 の完了を待たず、P1 が書き込んだレコードを P2 が即座に処理できる。

## 設計概要

### 変更箇所

1. **`dag_controller_impl`**: forward exchange ステップに対する状態遷移条件を変更し、
   上流プロセスが `completed` になるまで待たず、`running` になった時点で
   forward exchange ステップも `running` に移行させる。
2. **`take_flat`**: `next_record()` が false かつ `source_active()` が true の場合に yield して
   タスクをリスケジュールする実装を追加する。

## 初期見積もり

4d 

- コード変更は比較的単純
- dag_controller の状態遷移と yield 処理がプロセスや関係演算子に広範囲に影響するため、動作検証がメイン

## その他・検討事項

- limit 処理だけを先行してノンブロッキング化する案もあるが別途設計とする
  - 上流プロセスにプロセス完了を通知して途中で打ち切るという設計
- 本文書では追加部分の複雑さを考えると全体のノンブロッキング化を実施したほうがいいと判断した

## 参照ドキュメント

- [task-yield.md](task-yield.md): プロセスタスクの yield 処理全般の設計
- [scheduler_internals.md](scheduler_internals.md): タスクスケジューラの内部設計
- [process_executor_design.md](process_executor_design.md): プロセス実行機設計
