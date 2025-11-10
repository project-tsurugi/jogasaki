# 2025-10 max result setの設計と実装

## 目標

1つのクエリの実行に必要な最大writer数を固定し、それを超えるwriterが必要になる場合は縮退運転を行う
縮退運転では性能低下を許容する

## 設計

- writer poolをクエリごとに１つ保持する
- 最大ライター数を計算する既存の関数 calculate_max_writer_count を利用して、poolの最大メンバ数を決定する
  - 計算結果がconfiguration::max_result_set_writersを超える場合は、configuration::max_result_set_writersを最大メンバ数とする
  - poolの初期化時に最大個数のseatを作成して詰める
- writer poolはwriter_seatをキューで保持する。seatはchannelへの参照と予約状態を持つが、writer本体はacquire時ではなく、実際に使用される時に初めて作成される（遅延評価）。
    - channelからacquireしてwriter本体を格納してしまうとacquire時にメモリ確保が行われ、タスク実行の結果「結局使われなかったwriter」が無駄になる
    - emitを含むタスクの実行時に初めてseatからwriterを実体化する(同時にseatにwriterを保存する)
    - seatは「writerを1つ使用する権利」を表すもの。長さ1のコンテナ。

- writer poolは並列キュー(FIFO)
    - ただし将来的にはthread localによるキャッシュを保持するかもしれない
    - push/popのタイミングはtaskの開始と最後(yield直前)なので、あまり頻度が多くない、初動では不要

- タスクがemitを含むかどうかを判定できるようにする
    - emitを含む場合、seatをacquireしてtask contextに保持する
    - acquireに失敗した場合はyieldして次の実行をまつ
    - emitの実行時にtask contextからwriterを取り出して使う

## クラス設計

クラス名および主要なメンバ関数のみを記述する。

```
namespace jogasaki::executor::io {

/**
 * @brief represents a single seat that can hold a record_writer instance
 * @details A reserved seat models the right to use (acquire) a writer. 
 * Non-reserved seats are not associated with writers and they are just placeholders for writer_pool::acquire().
 * The internal writer instance is created lazily when first needed to avoid unnecessary allocations.
 * @note this object is not thread-safe and only one thread should access it at a time.
 */
class writer_seat {
public:

    /**
     * @brief construct new seat
     * @param channel optional channel used to materialize writers (may be nullptr)
     * @param reserved whether the seat is already reserved or not
     */
    explicit writer_seat(record_channel* channel = nullptr, bool reserved = false) noexcept;
    
    /**
     * @brief check whether the seat is reserved (i.e. holding the right to use/acquire a writer)
     * @return true if the seat is reserved
     * @return false otherwise
     */
    [[nodiscard]] bool reserved() const noexcept;
    
    /**
     * @brief acquire the new writer if necessary and return its reference 
     * @details after calling this function, the seat will hold the writer instance. The writer is
     * lazily acquired from the underlying channel the first time this function is called.
     * @return reference to the acquired writer
     * @pre the seat must be reserved. Otherwise, behavior is undefined.
     * @post after calling this function, has_writer() returns true.
     */
    std::shared_ptr<io::record_writer> const& writer();

    /**
     * @brief check whether the seat is occupied with the writer acquired
     * @details this function returns true if the seat already holds the writer instance, i.e. writer() has been called before.
     * @return true if the seat holds a writer
     * @return false otherwise
     */
    [[nodiscard]] bool has_writer() const noexcept;
};

/**
 * @brief a fixed-capacity FIFO pool of writer seats
 * @details The pool maintains a fixed number of seats created during
 * construction. 
 * @note this object is thread-safe and can be accessed from multiple threads concurrently.
 */
class writer_pool {
public:
    /**
     * @brief construct pool with given capacity
     * @param channel the record channel used to materialize writers
     * @param capacity the maximum number of reserved seats in the pool
     */
    writer_pool(record_channel& channel, std::size_t capacity);
    
    /**
     * @brief try to acquire a reserved seat from the pool
     * @param out the reserved seat that will receive the reserved seat state when available. Any
     * existing seat state in out will be move-assigned from the pool, including reserved status and
     * the lazily created writer (if present). When acquisition fails, out remains unchanged.
     * @return true when a seat was available and successfully reserved. The caller is responsible to
     * later return the seat using release(), otherwise the seat will leak.
     * @return false otherwise
     */
    bool acquire(writer_seat& out) noexcept;
    
    /**
     * @brief return a previously acquired seat back to the pool
     * @details the seat together with its writer (if any) is returned to the pool and becomes
     * available for future acquire() calls. Ownership is moved back into the pool's internal seat
     * instance so that the caller's seat becomes non-reserved and empty.
     * @param seat the seat to return. After this call, the parameter will be non-reserved seat and
     * hold no writer.
     */
    void release(writer_seat&& seat) noexcept;

    /**
     * @brief release any resource held by the pool
     * @details all the seats in the pool are cleared and any writers held by them are released.
     * @note after this call, the pool can no longer be used to acquire seats. Otherwise, behavior is undefined.
     */
    void release_pool();
};

}

```

## 実装

### 既存コードの変更点

- request_contextがwriter_poolをshared_ptrで保持(メンバ変数名writer_pool_)し、クエリの実行開始時にmake_sharedする。クエリでない場合はnullptr。
- impl::task_contextがwriter_seatを保持する(メンバ変数名writer_seat_)
- タスクの開始時、emitを含むタスクの場合にはimpl::task_context::writer_seat_にwriter_poolからacquireしたseatを保持する
  - acquireに失敗した場合はyieldする。つまりタスクの実行を中断し、そのタスクのコピーをスケジューラに登録する。
- タスクの完了直前、writer_seat_が保持しているseatをwriter_poolにreleaseする
- task::external_writer()はseatのwriter()を返すようにする
- emit内でemit_contextにexternal_writer()で取得したwriterを一時的に格納し、emit_context::release()で解放していたが、writerのオーナーはseatなので、emit_contextにwriterを格納しないように変更する
- ジョブの完了時、ジョブ完了コールバックを呼ぶ直前で、request_contextのwriter_pool_をrelease_pool()してリソースを解放する
  - コールバックでchannelのreleaseが呼ばれるので、その直前にwriterが解放されるようにする

### 既存ロジックの変更

- 既存のロジックではmax_result_set_writersの設定に基づいて、それを超えるような場合にクエリをエラーにしていたが、そのロジックはなくし、超える場合にはmax_result_set_writersを上限として縮退運転を行うようにする。

## 注意点

- 現状ではprocess_executor内部でseatをacquireしてreleaseするようにしている。将来的にINSERT文のようなprocess_executor経由で実行されないものが結果セットを戻す場合には、そちらにも別途seatのacquire/releaseを追加する必要がある。
- 現状ではINSERT/UPDATE文のような結果セットを戻さないステートメントや、クエリ(つまりemit演算子を含む)が結果セットを要求しないAPI (ExecuteStatement)で実行された際にnull_record_channelを作成して、「クエリかどうかにかかわらずステートメントの実行時にはかならずchannelが存在する」という状態にしている。このため、単純にchannelの有無ではemitを含むかどうかを判定できない。
  - そこで `create_request_context()` に `has_result_records` という引数を追加し、クエリ実行時にはtrueを渡すようにした。クエリが結果セットを要求しないAPIで実行された場合でもwriter_poolが作成されるが、このケースは性能要求が高いとは考えにくいため、許容する。