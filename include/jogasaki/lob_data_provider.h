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

#include <limits>
#include <memory>
#include <cstdlib>
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace jogasaki {

/**
 * @brief blob/clob data provider kind
 */
enum class lob_data_provider : std::int32_t {

    ///@brief kind undefined
    undefined = 0,

    ///@brief datastore provides lob data
    datastore = 1,

    ///@brief sql engine provides lob data
    sql = 2,

};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponded string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(lob_data_provider value) noexcept {
    using namespace std::string_view_literals;
    using kind = lob_data_provider;
    switch (value) {
        case kind::undefined: return "undefined"sv;
        case kind::datastore: return "datastore"sv;
        case kind::sql: return "sql"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, lob_data_provider value) {
    return out << to_string_view(value);
}

}  // namespace jogasaki
