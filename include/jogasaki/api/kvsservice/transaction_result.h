/*
 * Copyright 2018-2023 tsurugi project.
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

#include "status.h"

namespace jogasaki::api::kvsservice {

class transaction;

/**
 * @brief the transaction result for asynchronous begin/commit/abort.
 */
class transaction_result {
public:
    /**
     * @brief create new object
     * @param tx_status the operation status
     * @param tx the transaction of the operation.
     * tx should be nullptr only if begin transaction failed.
     */
    transaction_result(status tx_status, std::shared_ptr<transaction> tx) noexcept;

    transaction_result(transaction_result const &other) = delete;
    transaction_result &operator=(transaction_result const &other) = delete;
    transaction_result(transaction_result &&other) noexcept = default;
    transaction_result &operator=(transaction_result &&other) noexcept = default;

    /**
     * @brief destructor the object
     */
    ~transaction_result() = default;

    /**
     * @brief returns the transaction status.
     * @return the transaction status
     */
    [[nodiscard]] status tx_status() const noexcept;

    /**
     * @brief returns the transaction.
     * @return the transaction.
     * returns nullptr only if begin transaction failed.
     * returns valid transaction pointer even if other transactional operation failed.
     */
    [[nodiscard]] std::shared_ptr<transaction> &tx() const noexcept;

private:
    status tx_status_ {};
    std::shared_ptr<transaction> tx_ {};
};
}
