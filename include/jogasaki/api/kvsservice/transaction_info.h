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

#include <cstdint>

namespace jogasaki::api::kvsservice {

class transaction;

/**
 * @brief the information to specify a transaction
 */
class transaction_info {
public:
    transaction_info() = default;

    /**
     * @brief create new object
     * @param tx the transaction of this transaction_info
     */
    transaction_info(transaction *tx) noexcept;

    /**
     * @brief retrieves the system_id of this transaction
     * @return the system_id of rhe transaction
     */
    std::uint64_t system_id() const noexcept;

private:
    std::uint64_t system_id_ {};
};

}
