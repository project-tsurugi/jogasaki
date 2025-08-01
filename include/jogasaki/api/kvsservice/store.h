/*
 * Copyright 2018-2025 Project Tsurugi.
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

#include <cstdint>
#include <future>
#include <memory>
#include <tbb/concurrent_hash_map.h>

#include <sharksfin/api.h>

#include <jogasaki/api/database.h>
#include <jogasaki/api/kvsservice/status.h>
#include <jogasaki/api/resource/bridge.h>
#include <jogasaki/utils/split_mix64.h>

#include "transaction.h"
#include "transaction_option.h"


namespace jogasaki::api::kvsservice {

namespace details {

/**
 * @brief hash compare class to mix hash values using split mix 64
 */
class split_mix64_hash_compare {

public:

    [[nodiscard]] std::size_t hash(std::uint64_t const& a) const {
        return utils::split_mix64(a);
    }

    [[nodiscard]] bool equal(std::uint64_t const& a, std::uint64_t const& b) const {
        return a == b;
    }
};

}  // namespace details

class store {
public:
    /**
     * @brief create new object
     */
    store() = default;

    /**
     * @brief create new object
     * @param bridge the resource of Jogasaki
     */
    explicit store(std::shared_ptr<jogasaki::api::resource::bridge> const& bridge);

    store(store const &other) = delete;
    store &operator=(store const &other) = delete;
    store(store &&other) noexcept = delete;
    store &operator=(store &&other) noexcept = delete;

    /**
     * @brief destructor the object
     */
    ~store();

    /**
     * @brief begin the new transaction
     * The state of the returned transaction is usually transaction_state::state_kind::started,
     * but sometimes it can be waiting_start.
     * You should check whether the state is started, or wait until the state is started.
     * Requesting transaction operations is permitted after the state is started.
     * You can check the state of the transaction by transaction::state().
     * @param option transaction option
     * @param tx [out] the transaction filled when successful
     * @return status::ok when successful
     * @return any other error otherwise
     * @see transaction::state()
     */
    [[nodiscard]] status begin_transaction(transaction_option const& option,
                                           std::shared_ptr<transaction>& tx);

    /**
     * @brief find the transaction with the system_id.
     * system_id should be the return value of transaction::system_id().
     * This method is thread-safe.
     * @param system_id system_id of the transaction
     * @return valid shared_ptr with the transaction if exists
     * @return shared_ptr with nullptr is not exists
     * @see transaction::system_id()
     */
    [[nodiscard]] std::shared_ptr<transaction> find_transaction(std::uint64_t system_id);

    /**
     * @brief dispose the transaction
     * If the transaction is still running (e.g. commit/abort has not been requested
     * and no abort condition has been met with APIs), the transaction will be aborted
     * and then disposed.
     * You should call this method after the transaction object is needless,
     * should not use the disposed transaction object after calling this method.
     * @param system_id system_id of the transaction
     * @return status::ok when successful
     * @return any other error otherwise
     */
    [[nodiscard]] status dispose_transaction(std::uint64_t system_id);
private:
    jogasaki::api::database *db_{};
    sharksfin::DatabaseHandle db_handle_{};
    tbb::concurrent_hash_map<std::uint64_t, std::shared_ptr<transaction>, details::split_mix64_hash_compare> transactions_{};
};

}
