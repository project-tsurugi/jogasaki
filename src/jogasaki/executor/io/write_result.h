/*
 * Copyright 2018-2026 Project Tsurugi.
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

#include <cstdlib>
#include <ostream>
#include <string_view>

namespace jogasaki::executor::io {

/**
 * @brief the result of writing a record with record_writer
 */
enum class write_result {
    /**
     * @brief the record is written successfully
     */
    ok,

    /**
     * @brief the record is accepted (written or dropped), and the destination requires no more records
     * @details the writer can continue to accept records, but they are dropped.
     * For example, the forward exchange with row count returns this when it has received the records of the count.
     */
    satisfied,

    /**
     * @brief the record is not written due to an error
     */
    error,
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponding string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(write_result value) noexcept {
    using namespace std::string_view_literals;
    switch (value) {
        case write_result::ok: return "ok"sv;
        case write_result::satisfied: return "satisfied"sv;
        case write_result::error: return "error"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, write_result value) {
    return out << to_string_view(value);
}

}  // namespace jogasaki::executor::io
