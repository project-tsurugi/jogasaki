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

#include <limits>
#include <memory>
#include <cstdlib>
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace jogasaki::lob {

/**
 * @brief blob/clob reference kind
 */
enum class lob_reference_kind : std::int32_t {

    ///@brief kind undefined
    undefined = 0,

    ///@brief lob data is provided by caller as a parameter
    ///@details lob data is not registered and no id is assigned. Only a locator is associated.
    provided,

    ///@brief lob reference is fetched from datastore and ready for reading
    ///@details lob data has been registered and id is assigned. No locator is associated.
    ///For writing, the id needs to be duplicated for re-registration.
    fetched,

    ///@brief lob reference is resolved and ready to for reading/writing
    ///@details lob data has been registered and id is assigned.
    resolved,
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponding string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(lob_reference_kind value) noexcept {
    using namespace std::string_view_literals;
    using kind = lob_reference_kind;
    switch (value) {
        case kind::undefined: return "undefined"sv;
        case kind::provided: return "provided"sv;
        case kind::fetched: return "fetched"sv;
        case kind::resolved: return "resolved"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, lob_reference_kind value) {
    return out << to_string_view(value);
}

}  // namespace jogasaki::lob
