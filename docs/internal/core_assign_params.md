# ワーカースレッド割当の構成

2025-04-25 kurosawa

## 本文書について

jogasakiのタスクスケジューラがワーカースレッドをNUMAノードやCPUコアに割当てる仕組みと構成パラメータについて説明する

## ワーカースレッドのノードやコアへの割当て

- jogasakiタスクスケジューラはワーカースレッドをNUMAノードやCPUコアに固定的に割当てて動作させることが可能
  - ただし現時点のデフォルトはこれを行っておらず、ワーカスレッドが動作するノードやコアはOSに任せられている

- ワーカーの割当て対象をNUMAノード単位で行うか、CPUコア単位で行うかという2つの種類がある
  - 本文書では前者を **ノードアサイン** 、後者を **コアアサイン** と呼ぶ

- 割当て機能 (カッコ内は関連する構成パラメータ)
  - コアアサイン
    - 特定のコアから連番でCPUを割当てる(`core_affinity=true`)
  - ノードアサイン
    - NUMAノードに対して均一にワーカーを割当てる(`assign_numa_nodes_uniformly=true`)
    - 特定のNUMAノードですべてのワーカーを稼働させる(`force_numa_node`)

## 構成パラメータ

ワーカースレッド割当てに関連する `jogasaki::configuration` のプロパティを下記に示す。

`tsurugi.ini` に設定する名前はこれらに `dev_` サフィックスがつくことがある。詳細は [config_parameters.md](https://github.com/project-tsurugi/tateyama/blob/master/docs/config_parameters.md)を参照

- `core_affinity` 

  - コアアサインを実施するかどうかを `true`/`false` で指定
  - `force_numa_node` が未設定かつ `assign_numa_nodes_uniformly=false` の場合のみ有効

- `initial_core`

  - コアアサインを実施する際に割当てを開始する最初の(論理)コア番号 (0-origin)
  - デフォルトは1 
  - この番号から連番で `thread_pool_size` 個のワーカーがコアアサインされる
  - `core_affinity=true` の場合のみ有効

- `assign_numa_nodes_uniformly`

  - ノードアサインを行う。全NUMAノードへ均等になるように、`thread_pool_size` 個の全ワーカーをラウンドロビンで各ノードへ割当てる
  - `force_numa_node` が未設定かつ `core_affinity=false` の場合のみ有効

- `force_numa_node`

  - ノードアサインを行う。特定のNUMAノードの番号(0-origin)を指定して、全ワーカーがその単一NUMAノードで稼働するようにする
  - `assign_numa_nodes_uniformly=false` かつ `core_affinity=false` の場合のみ有効

## 注意事項

- 本機能は性能実験用のため詳細設定は最低限、エラーハンドリングも限定的である
- 0番のコアは特殊な用途に使われるケースがあったため `initial_core` のデフォルト値は0でなく1である。
- jogasakiから認識するコアは論理コアであるため、設定によっては物理コアを共有する論理コアを割当ててしまう可能性がある
  - 例えば `force_numa_node` を指定したノードの物理コア数が `thread_pool_size` より小さい場合
