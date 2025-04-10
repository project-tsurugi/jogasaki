/*
 * Copyright 2018-2023 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string_view>

#include <sharksfin/CallResult.h>
#include <sharksfin/api.h>

#include <jogasaki/commit_profile.h>
#include <jogasaki/commit_response.h>
#include <jogasaki/error/error_info.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/transaction.h>
#include <jogasaki/kvs/transaction_option.h>
#include <jogasaki/status.h>
#include <jogasaki/utils/interference_size.h>

namespace limestone::api {

class blob_pool;

}

namespace jogasaki {


namespace details {

/**
 * @brief termination state
 * @details task use count and going-to-{commit, abort} flags packed in unsigned
 * 64-bit integer
 */
class termination_state {

public:

    //@brief bit mask for going-to-abort flag
    static constexpr std::uint64_t bit_mask_going_to_abort = 1UL << 63U;

    //@brief bit mask for going-to-commit flag
    static constexpr std::uint64_t bit_mask_going_to_commit = 1UL << 62U;

    //@brief bit mask for task use count
    static constexpr std::uint64_t bit_mask_task_use_count = bit_mask_going_to_commit - 1;

    /**
     * @brief construct new object
     */
    constexpr termination_state() = default;

    termination_state(termination_state const&) = default;
    termination_state(termination_state&&) = default;
    termination_state& operator=(termination_state const&) = default;
    termination_state& operator=(termination_state&&) = default;

    /**
     * @brief destruct object
     */
    ~termination_state() = default;

    /**
     * @brief task use count accessor
     * @return the number of in-transaction tasks using the transaction context
     */
    [[nodiscard]] std::size_t task_use_count() const noexcept {
        return static_cast<std::size_t>(state_ & bit_mask_task_use_count);
    }

    /**
     * @brief task use count setter
     * @param value the new task use count
     */
    void task_use_count(std::size_t value) noexcept {
        state_ = (state_ & ~bit_mask_task_use_count) | (value & bit_mask_task_use_count);
    }

    /**
     * @brief accessor for the going-to-abort flag
     * @return true if the transaction is going to abort, false otherwise
     */
    [[nodiscard]] bool going_to_abort() const noexcept {
        return (state_ & bit_mask_going_to_abort) != 0;
    }

    /**
     * @brief set the going-to-abort flag
     */
    void set_going_to_abort() noexcept {
        state_ |= bit_mask_going_to_abort;
    }

    /**
     * @brief accessor for the going-to-commit flag
     * @return true if the transaction is going to commit, false otherwise
     */
    [[nodiscard]] bool going_to_commit() const noexcept {
        return (state_ & bit_mask_going_to_commit) != 0;
    }

    /**
     * @brief set the going-to-commit flag
     */
    void set_going_to_commit() noexcept {
        state_ |= bit_mask_going_to_commit;
    }

    /**
     * @brief getter for whole value for flags and counter
     */
    explicit operator std::uint64_t() const noexcept {
        return state_;
    }

    /**
     * @brief clear the state
     */
    void clear() noexcept {
        state_ = 0;
    }

    /**
     * @brief check if in-transaction task exists using the transaction context
     * @return true if no in-transaction task exists using the transaction
     * context, false otherwise
     */
    [[nodiscard]] bool task_empty() const noexcept {
        return task_use_count() == 0;
    }

private:
    std::uint64_t state_{};
};

static_assert(std::is_trivially_copyable_v<termination_state>);
static_assert(std::is_trivially_destructible_v<termination_state>);
static_assert(std::alignment_of_v<termination_state> == 8);
static_assert(sizeof(termination_state) == 8);

inline std::uint32_t upper(std::uint64_t arg) {
    return static_cast<std::uint32_t>(arg >> 32U);
}

inline std::uint32_t lower(std::uint64_t arg) {
    return static_cast<std::uint32_t>(arg & ((1UL << 32U)-1));
}

/**
 * @brief transaction termination manager
 */
class cache_align termination_manager {
public:

    /**
     * @brief try to set the going-to-abort flag
     * @details atomically do the following: check both going-to-abort and going-to-commit flags are not set,
     * and then set the going-to-abort flag returning the final state.
     * @param ts [out] the termination state set by this call
     * @return true if the going-to-abort flag is set successfully
     * @return false if any of the flags are already set
     */
    bool try_set_going_to_abort(termination_state& ts);

    /**
     * @brief try to set the going-to-commit flag
     * @param ts [out] the termination state set by this call
     * @details atomically do the following: check both going-to-abort and going-to-commit flags are not set,
     * and then set the going-to-commit flag returning the final state.
     * @return true if the going-to-commit flag is set successfully
     * @return false if any of the flags are already set
     */
    bool try_set_going_to_commit(termination_state& ts);

    /**
     * @brief try to increment the task use count
     * @param ts [out] the termination state set by this call
     * @details atomically do the following: check both going-to-abort and
     * going-to-commit flags are not set, and then increment the task use count returning the final state.
     * @return true if the task use count is incremented successfully
     * @return false if any of the flags are already set
     */
    bool try_increment_task_use_count(termination_state& ts);

    /**
     * @brief decrement the task use count
     * @param ts [out] the termination state set by this call
     * @details atomically decrement the task use count and return the final
     * state.
     */
    void decrement_task_use_count(termination_state& ts);

    /**
     * @brief get the current termination state
     * @return the current termination state
     */
    [[nodiscard]] termination_state state() const noexcept;

private:
    std::atomic<termination_state> state_{};
};

/**
 * @brief worker and its use count manager
 */
class cache_align worker_manager {
public:
    constexpr static std::uint32_t empty_worker = static_cast<std::uint32_t>(-1);

    /**
     * @brief check worker is same as given index and increment use count if no worker is assigned or given worker index is same as one already assigned
     * @param worker_index [in/out] the worker index to increment use count or assign. If this call returns false, already assigned worker index is returned.
     * @return true if use count increment is successful (including the case when use count becomes 1 from 0 as new worker is assigned)
     * @return false if use count increment is not successful (given worker_index is different from one already assigned)
     */
    bool increment_and_set_on_zero(std::uint32_t& worker_index);

    /**
     * @brief decrement the use count for the currently assigned worker and un-assign if it reaches zero
     * @return true use count reaches zero and worker is un-assigned
     * @return false use count decremented, but didn't reach zero
     */
    bool decrement_and_clear_on_zero();

    /**
     * @brief accessor to worker id
     * @return worker id if it's already assigned
     * @return `empty_worker` otherwise
     */
    [[nodiscard]] std::uint32_t worker_id() const noexcept;

    /**
     * @brief accessor assigned worker's use count
     * @return use count of the assigned worker
     * @return zero if no worker is assigned for this manager
     */
    [[nodiscard]] std::uint32_t use_count() const noexcept;

private:
    std::atomic_size_t use_count_and_worker_id_{empty_worker & ((1UL << 32U)-1)};
};

} // namespace details

/**
 * @brief context object for the transaction scope
 * @details this class represents context information in the scope of the transaction.
 * This contains more state/context than kvs::transaction, which is the low level I/O abstraction.
 * This represents more state information in the jogasaki layer, such as simultaneous requests
 * sharing the kvs::transaction. Since kvs::transaction is not thread-safe, this object wraps it and provide
 * accessor to necessary context such as worker count. Using the context, callers need to decide which worker/thread
 * can use the transaction.
 */
class transaction_context {
public:
    using mutex_type = std::recursive_mutex;

    using durability_marker_type = ::sharksfin::durability_marker_type;

    using commit_callback_type = kvs::transaction::commit_callback_type;

    using clock = std::chrono::system_clock; // we need to know the current time, so steady_clock is not suitable

    /**
     * @brief create empty object
     */
    transaction_context() = default;

    /**
     * @brief destruct object
     */
    ~transaction_context() noexcept;

    transaction_context(transaction_context const& other) = delete;
    transaction_context& operator=(transaction_context const& other) = delete;
    transaction_context(transaction_context&& other) noexcept = delete;
    transaction_context& operator=(transaction_context&& other) noexcept = delete;

    /**
     * @brief create new context object
     * @param transaction the kvs transaction used in this context
     * @param transaction the kvs transaction option that was used to create `transaction`
     */
    explicit transaction_context(
        std::shared_ptr<kvs::transaction> transaction,
        std::shared_ptr<kvs::transaction_option const> option = nullptr
    );

    [[nodiscard]] operator kvs::transaction&() const noexcept;  //NOLINT

    /**
     * @brief accessor for the wrapped transaction
     * @return transaction object in the kvs layer
     */
    [[nodiscard]] std::shared_ptr<kvs::transaction> const& object() const;

    /**
     * @brief the transaction id
     * @return the id to uniquely identify the transaction context
     */
    [[nodiscard]] std::size_t id() const noexcept;

    /**
     * @brief check if the object is valid
     * @return true if the object contains valid transaction
     * @return false otherwise
     */
    [[nodiscard]] explicit operator bool() const noexcept;

    /**
     * @see kvs::transaction::commit()
     */
    [[nodiscard]] status commit(bool async = false);

    /**
     * @see kvs::transaction::commit()
     */
    [[nodiscard]] bool commit(commit_callback_type cb);

    /**
     * @see kvs::transaction::abort_transaction()
     */
    [[nodiscard]] status abort_transaction();

    /**
     * @see kvs::transaction::control_handle()
     */
    [[nodiscard]] sharksfin::TransactionControlHandle control_handle() const noexcept;

    /**
     * @see kvs::transaction::handle()
     */
    [[nodiscard]] sharksfin::TransactionHandle handle() noexcept;

    /**
     * @see kvs::transaction::database()
     */
    [[nodiscard]] kvs::database* database() const noexcept;

    /**
     * @brief return the detailed info for the recent kvs api call
     * @return recent api call result
     * @return nullptr if result is not available
     */
    [[nodiscard]] std::shared_ptr<sharksfin::CallResult> recent_call_result() noexcept;

    /**
     * @brief check worker is same as given index and increment use count if no worker is assigned or given worker index is same as one already assigned
     * @param worker_index [in/out] the worker index to increment use count or assign. If this call returns false, already assigned worker index is returned.
     * @return true if use count increment is successful (including the case when use count becomes 1 from 0 as new worker is assigned)
     * @return false if use count increment is not successful (given worker_index is different from one already assigned)
     */
    bool increment_worker_count(std::uint32_t& worker_index);

    /**
     * @brief decrement the use count for the currently assigned worker and un-assign if it reaches zero
     * @return true use count reaches zero and worker is un-assigned
     * @return false use count decremented, but didn't reach zero
     */
    bool decrement_worker_count();

    /**
     * @brief accessor to the mutex for transaction lock
     * @return the mutex
     */
    mutex_type& mutex() noexcept;

    /**
     * @brief check if transaction is already assigned to epoch and ready for request
     * @return true when transaction is ready
     * @return false otherwise
     */
    bool is_ready();

    /**
     * @brief return the transaction id
     * @return transaction id string
     * @return empty string when it's not available
     */
    [[nodiscard]] std::string_view transaction_id() noexcept;

    /**
     * @brief setter for the error info
     * @details only the first one is stored and subsequent error info (that comes late) is ignored
     * @param the error info to be stored
     * @return true if the given error info is stored
     * @return false if the given error info is not stored because error info is already set.
     * @note this function is thread-safe and multiple threads can simultaneously call error_info setter/getter.
     *
     */
    bool error_info(std::shared_ptr<error::error_info> const& info) noexcept;

    /**
     * @brief accessor for the error info
     * @return the error info for the request result
     * @return nullptr if no error occurs
     * @note this function is thread-safe and multiple threads can simultaneously call error_info setter/getter.
     */
    [[nodiscard]] std::shared_ptr<error::error_info> error_info() const noexcept;

    /**
     * @brief accessor for the commit_response value
     * @return the commit response kind indicating the timing when commit completion is notified
     * @note this function is not thread-safe - multiple threads should not call commit_response
     * setter/getter simultaneously.
     */
    [[nodiscard]] commit_response_kind commit_response() const noexcept;

    /**
     * @brief setter for the commit_response value
     * @param arg the commit_response value to be set
     * @note this function is not thread-safe - multiple threads should not call commit_response
     * setter/getter simultaneously.
     */
    void commit_response(commit_response_kind arg) noexcept;

    /**
     * @brief accessor for the durability marker value
     * @return the durability marker set for this transaction
     * @return nullopt if marker is not set (e.g. pre-commit not yet completed)
     * @note this function is not thread-safe - multiple threads should not call durability_marker
     * setter/getter simultaneously.
     */
    [[nodiscard]] std::optional<durability_marker_type> durability_marker() const noexcept;

    /**
     * @brief setter for the durability marker value
     * @param arg the durability marker value to be set
     * @note this function is not thread-safe - multiple threads should not call durability_marker
     * setter/getter simultaneously.
     */
    void durability_marker(std::optional<durability_marker_type> arg) noexcept;

    /**
     * @brief accessor for the commit profile
     * @return the profiling information for the commit of this transaction
     */
    [[nodiscard]] std::shared_ptr<commit_profile> const& profile() const noexcept;

    /**
     * @brief accessor for the transaction option
     * @return the transaction option
     */
    [[nodiscard]] std::shared_ptr<kvs::transaction_option const> const& option() const noexcept;

    void start_time(clock::time_point arg) noexcept;
    [[nodiscard]] std::optional<clock::time_point> start_time() const noexcept;

    void end_time(clock::time_point arg) noexcept;
    [[nodiscard]] std::optional<clock::time_point> end_time() const noexcept;

    template<class Duration>
    [[nodiscard]] Duration duration() const noexcept {
        if (! start_time_.has_value() || ! end_time_.has_value()) {
            return {};
        }
        return std::chrono::duration_cast<Duration>(end_time_.value() - start_time_.value());
    }

    void label(std::string_view arg) noexcept;

    [[nodiscard]] std::string_view label() const noexcept;

    /**
     * @brief accessor for the blob pool
     * @return the blob pool set for this transaction
     * @return nullptr if blob pool is not set
     * @note this function is not thread-safe - multiple threads should not call
     * setter/getter simultaneously.
     */
    [[nodiscard]] std::shared_ptr<limestone::api::blob_pool> const& blob_pool() const noexcept {
        return blob_pool_;
    }

    /**
     * @brief setter for the blob pool value
     * @param arg the blob pool to be set
     * @note this function is not thread-safe - multiple threads should not call
     * setter/getter simultaneously.
     */
    void blob_pool(std::shared_ptr<limestone::api::blob_pool> arg) noexcept {
        blob_pool_ = std::move(arg);
    }

    /**
     * @brief accessor to the mutex for transaction lock
     * @return the mutex
     */
    details::termination_manager& termination_mgr() noexcept {
        return term_mgr_;
    }

private:
    std::shared_ptr<kvs::transaction> transaction_{};
    std::size_t id_{};
    details::worker_manager mgr_{};
    mutex_type mutex_{};
    std::shared_ptr<error::error_info> error_info_{};
    commit_response_kind commit_response_{commit_response_kind::undefined};
    std::optional<durability_marker_type> durability_marker_{};
    std::shared_ptr<commit_profile> profile_{std::make_shared<commit_profile>()};
    std::shared_ptr<kvs::transaction_option const> option_{};
    std::optional<clock::time_point> start_time_{};
    std::optional<clock::time_point> end_time_{};
    std::string label_{};
    std::shared_ptr<limestone::api::blob_pool> blob_pool_{};
    details::termination_manager term_mgr_{};

    static inline std::atomic_size_t id_source_{};  //NOLINT
};

/**
 * @brief wrap the kvs::transaction and convert into transaction context
 * @param arg the kvs::transaction object
 * @return the converted context
 */
std::shared_ptr<transaction_context> wrap(
    std::unique_ptr<kvs::transaction>&& arg,
    std::shared_ptr<kvs::transaction_option const> options = nullptr
) noexcept;

}

