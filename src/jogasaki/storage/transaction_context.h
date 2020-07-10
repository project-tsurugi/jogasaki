/*
 * Copyright 2018-2020 tsurugi project.
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

#include <glog/logging.h>
#include <takatori/util/fail.h>
#include <sharksfin/api.h>

namespace jogasaki::storage {

using ::takatori::util::fail;

class storage_context;
/**
 * @brief context for the transaction
 */
class transaction_context {
public:
    /**
     * @brief create default context object
     */
    explicit transaction_context(storage_context& stg);

    /**
     * @brief create default context object
     */
    ~transaction_context() noexcept;

    transaction_context(transaction_context const& other) = default;
    transaction_context& operator=(transaction_context const& other) = default;
    transaction_context(transaction_context&& other) noexcept = default;
    transaction_context& operator=(transaction_context&& other) noexcept = default;

    bool commit();

    bool abort();

    [[nodiscard]] sharksfin::TransactionControlHandle control_handle() const noexcept;

    sharksfin::TransactionHandle handle() noexcept;

    void open_scan();

    bool next_scan();

    void close_scan();
private:
    sharksfin::TransactionControlHandle tx_{};
    sharksfin::TransactionHandle handle_{};
    sharksfin::IteratorHandle iterator_{};
    storage_context* parent_{};
    bool active_{true};
};

}

