/*
 * Copyright 2018-2022 tsurugi project.
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
#include <memory>

#include <jogasaki/kvs/database.h>

namespace jogasaki {

namespace details {

class worker_manager {
public:
    constexpr static std::uint32_t empty_worker = static_cast<std::uint32_t>(-1);

    bool increment_and_set_on_zero(std::uint32_t& worker_index) {
        std::size_t cur = use_count_and_worker_id_.load();
        std::size_t next{};
        std::uint32_t cnt = 0;
        std::uint32_t wid = 0;
        do {
            cnt = static_cast<std::uint32_t>(cur >> 32U);
            wid = static_cast<std::uint32_t>(cur & ((1UL << 32U)-1));
            if (cnt == 0) {
                wid = worker_index;
            }
            next = cnt + 1;
            next <<= 32U;
            next |= wid;
        } while(! use_count_and_worker_id_.compare_exchange_strong(cur, next));
        if(cnt == 0) {
            return true;
        }
        worker_index = wid;
        return false;
    }

    bool decrement_and_clear_on_zero() {
        std::size_t cur = use_count_and_worker_id_.load();
        std::size_t next{};
        std::uint32_t cnt = 0;
        std::uint32_t wid = 0;
        do {
            cnt = static_cast<std::uint32_t>(cur >> 32U);
            wid = static_cast<std::uint32_t>(cur & ((1UL << 32U)-1));
            if (cnt == 1) {
                wid = empty_worker;
            }
            next = cnt - 1;
            next <<= 32U;
            next |= wid;
        } while(! use_count_and_worker_id_.compare_exchange_strong(cur, next));
        return cnt == 1;
    }

    [[nodiscard]] std::uint32_t worker_id() const noexcept {
        std::size_t cur = use_count_and_worker_id_.load();
        return static_cast<std::uint32_t>(cur & ((1UL << 32U)-1));
    }
    [[nodiscard]] std::uint32_t use_count() const noexcept {
        std::size_t cur = use_count_and_worker_id_.load();
        return static_cast<std::uint32_t>(cur >> 32U);
    }

private:
    std::atomic_size_t use_count_and_worker_id_{empty_worker & ((1UL << 32U)-1)};
};

}

/**
 * @brief context object for the transaction scope
 * @details this class represents context information in the scope of the transaction.
 * This contains more state/context than kvs::transaction, which is the low level I/O abstraction.
 * This represents more state information in the jogasaki layer, such as simultaneous requests
 * sharing the kvs::transaction.
 */
class transaction_context {
public:
    /**
     * @brief create empty object
     */
    transaction_context() = default;

    /**
     * @brief create new context object
     * @param transaction the kvs transaction used in this context
     */
    explicit transaction_context(
        std::shared_ptr<kvs::transaction> transaction
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
     * @see kvs::transaction::wait_for_commit()
     */
    [[nodiscard]] status wait_for_commit(std::size_t timeout_ns = 0UL);

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

    bool increment_worker_count(std::uint32_t& worker_index) {
        return mgr_.increment_and_set_on_zero(worker_index);
    }

    bool decrement_worker_count() {
        return mgr_.decrement_and_clear_on_zero();
    }
private:
    std::shared_ptr<kvs::transaction> transaction_{};
    std::size_t id_{};
    details::worker_manager mgr_{};

    static inline std::atomic_size_t id_source_{};  //NOLINT
};

/**
 * @brief wrap the kvs::transaction and convert into transaction context
 * @param arg the kvs::transaction object
 * @return the converted context
 */
std::shared_ptr<transaction_context> wrap(std::unique_ptr<kvs::transaction> arg) noexcept;

}

