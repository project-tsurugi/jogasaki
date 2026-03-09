# checkpoint_holder / lazy_checkpoint_holder

## 本文書について

プロセスの varlen buffer 領域を効率よく管理するための RAII ユーティリティクラス `checkpoint_holder` および `lazy_checkpoint_holder` の使い方に関する開発者向けメモ。

varlen buffer の概要については [varlen_buffer_resource.md](varlen_buffer_resource.md) を参照のこと。

---

## 概要

`lifo_paged_memory_resource` は LIFO 順でメモリを解放できる。チェックポイントを記録しておくことで、そのチェックポイント以降に確保されたメモリをまとめて解放 (rewind) することができる。

この機能を扱いやすいインタフェースで提供するのが `checkpoint_holder` と `lazy_checkpoint_holder` である。どちらを使うかは、チェックポイントの生存区間がスタックと対応するかどうかによって決まる。

---

## checkpoint_holder

**対象**: チェックポイントの生存区間がスタックのスコープ (RAII) と一致する場合。

```cpp
utils::checkpoint_holder cp{varlen_resource};
// ... varlen_resource からメモリを確保して処理 ...
// スコープを抜けると自動的に rewind される
```

コンストラクタでチェックポイントを記録し、デストラクタでそのチェックポイント位置まで rewind する。

### reset()

スコープを抜ける前に明示的に rewind したい場合は `reset()` を使う。`reset()` を呼んだ後にデストラクタが実行されても二重解放にはならない（`reset()` は冪等）。

```cpp
utils::checkpoint_holder cp{varlen_resource};
// ...
cp.reset();  // 明示的に rewind
// デストラクタは no-op
```

ループ内でイテレーションごとに rewind するケースでも同様に `reset()` を使う。

```cpp
utils::checkpoint_holder cp{varlen_resource};
for (auto& row : rows) {
    process(row, varlen_resource);
    cp.reset();  // このイテレーションで確保した varlen data を解放し、次のイテレーションへ
}
```

---

## lazy_checkpoint_holder

**対象**: チェックポイントの生存区間がスタックのスコープと対応しない場合。yield/resume をまたいで使用するケースが典型例。

`lazy_checkpoint_holder` はコンストラクタでチェックポイントを記録せず、デストラクタでも rewind しない。チェックポイントの管理はすべてユーザーの責任となる。

```cpp
utils::lazy_checkpoint_holder cp{varlen_resource};
cp.set_checkpoint();           // ここを開始点として記録
// ... 処理 ...
cp.reset();                    // 開始点まで rewind し、チェックポイントを解除
```

### メソッド

| メソッド | 説明 |
|---|---|
| `set_checkpoint()` | 現在位置をチェックポイントとして記録する。チェックポイントが設定済みの状態で再度呼ぶと `assert_with_exception` でフェイルする |
| `release()` | チェックポイント位置まで rewind する。チェックポイントは解除しない（次の `release()` でも同じ位置まで rewind できる）|
| `reset()` | `release()` を実行してからチェックポイントを解除する。構築直後の状態に戻る |
| `unset()` | rewind せずにチェックポイントを解除する |

### ループ内での使い方

チェックポイントを維持したまま各イテレーションの末尾で rewind する場合は `release()` を使う。

```cpp
cp.set_checkpoint();
while (reader.next_record()) {
    process(reader.get_record(), varlen_resource);
    cp.release();              // このイテレーションで確保した varlen data を解放
}
cp.reset();                    // ループ終了後、チェックポイント自体も解除
```

ループを途中で打ち切る場合（abort）もチェックポイントは不要になるため `reset()` を使う。

```cpp
while (reader.next_record()) {
    auto st = downstream->process_record(context);
    if (st == aborted) {
        cp.reset();            // abort パスでも rewind してチェックポイントを解除
        return aborted;
    }
    cp.release();
}
cp.reset();
```

### yield/resume をまたぐ場合

yield 時にはチェックポイントを維持したまま処理を中断する必要がある。yield 時には `release()` も `reset()` も呼ばないこと。resume 後に処理を継続し、下流の処理が完了した後で `release()` または `reset()` を呼ぶ。

```cpp
cp.set_checkpoint();
while (reader.next_record()) {
    process(reader.get_record(), varlen_resource);

// yield_point:
    ctx.state(context_state::calling_child);
    auto st = downstream->process_record(context);
    if (st == yield) {
        return yield;          // チェックポイントはそのまま維持。resume 後にここから再開
    }
    ctx.state(context_state::running_operator_body);

    cp.release();              // 下流が完了したので varlen data を解放
}
cp.reset();
```

yield 前に `release()` や `reset()` を呼んでしまうと、下流の演算子がまだ参照している varlen buffer 領域が解放されてメモリ破壊が発生する。

### set_checkpoint() を複数回呼ぶのは誤り

`set_checkpoint()` を `reset()` を挟まずに複数回呼ぶことは誤った使い方である。
スタック上のRAIIオブジェクトと同様に、開始点と終了点によって作られる区間があり、その範囲内で必要に応じて `release()` でrewindするというデザインである。
`set_checkpoint()` と `reset()` がそれぞれ開始点と終了点に対応する。
