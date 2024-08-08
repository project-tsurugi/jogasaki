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

namespace jogasaki {

namespace details {

inline std::uint32_t upper(std::uint64_t arg) {
    return static_cast<std::uint32_t>(arg >> 32U);
}

inline std::uint32_t lower(std::uint64_t arg) {
    return static_cast<std::uint32_t>(arg & ((1UL << 32U)-1));
}

/**
 * @brief worker and its use count manager
 */
class worker_manager {
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

    using clock = std::chrono::steady_clock;

    /**
     * @brief create empty object
     */
    transaction_context() = default;

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
     * @see kvs::transaction::abort()
     */
    [[nodiscard]] status abort();

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
    clock::time_point start_time() const noexcept;

    void end_time(clock::time_point arg) noexcept;
    clock::time_point end_time() const noexcept;

    template<class Duration>
    [[nodiscard]] Duration duration() const noexcept {
        return std::chrono::duration_cast<Duration>(end_time_ - start_time_);
    }

    void label(std::string_view arg) noexcept;

    [[nodiscard]] std::string_view label() const noexcept;

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
    clock::time_point start_time_{};
    clock::time_point end_time_{};
    std::string label_{};

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

