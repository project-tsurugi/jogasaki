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

#include <takatori/util/enum_set.h>

namespace jogasaki {

/**
 * @brief internal transaction state
 */
enum class transaction_state_kind : std::int32_t {
    /**
     * @brief undefined state
     */
    undefined = 0,

    /**
     * @brief initialized state
     * @details transaction is created but is not yet provided to the client
     */
    init,

    /**
     * @brief active state
     * @details transaction is provided to the client and actively accepts operations
     */
    active,

    /**
     * @brief going-to-commit state
     * @details sql engine received commit request, but it's not yet notified to cc
     */
    going_to_commit,

    /**
     * @brief cc-committing state
     * @details request has been sent to cc and is on-going
     */
    cc_committing,

    /**
     * @brief committed available state
     * @details cc has committed the transaction and it's available for read/write
     */
    committed_available,

    /**
     * @brief committed stored state
     * @details the transaction has been committed and stored durably in the database
     */
    committed_stored,

    /**
     * @brief going-to-abort state
     * @details sql engine received abort request, but it's not yet notified to cc
     */
    going_to_abort,

    /**
     * @brief aborted state
     * @details transaction has been aborted
     */
    aborted,

    /**
     * @brief unknown state
     */
    unknown,
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponding string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(transaction_state_kind value) noexcept {
    using namespace std::string_view_literals;
    using kind = transaction_state_kind;
    switch (value) {
        case kind::undefined: return "undefined"sv;
        case kind::init: return "init"sv;
        case kind::active: return "active"sv;
        case kind::going_to_commit: return "going_to_commit"sv;
        case kind::cc_committing: return "cc_committing"sv;
        case kind::committed_available: return "committed_available"sv;
        case kind::committed_stored: return "committed_stored"sv;
        case kind::going_to_abort: return "going_to_abort"sv;
        case kind::aborted: return "aborted"sv;
        case kind::unknown: return "unknown"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, transaction_state_kind value) {
    return out << to_string_view(value);
}

/// @brief a set of transaction_state_kind
using transaction_state_kind_set = takatori::util::enum_set<
        transaction_state_kind,
        transaction_state_kind::undefined,
        transaction_state_kind::unknown>;

}

