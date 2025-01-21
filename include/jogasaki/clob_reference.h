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

#include <cstdint>
#include <type_traits>

#include "lob_data_provider.h"
#include "lob_id.h"

namespace jogasaki {

/**
 * @brief clob field data object
 * @details Trivially copyable immutable class holding clob reference.
 */
class clob_reference {
public:
    /**
     * @brief default constructor representing empty object
     */
    constexpr clob_reference() = default;

    /**
     * @brief construct new object allocating from the given memory resource when long format is needed
     * @param resource memory resource used to allocate storage for long format
     * @param data pointer to the data area that is copied into tne new object
     * @param size size of the data area
     */
    clob_reference(clob_id_type id, lob_data_provider provider) :
        id_(id),
        provider_(provider)
    {}

    /**
     * @brief compare two clob object references
     * @param a first arg to compare
     * @param b second arg to compare
     * @return true if a == b
     * @return false otherwise
     */
    friend bool operator==(clob_reference const& a, clob_reference const& b) noexcept {
        return a.id_ == b.id_ && a.provider_ == b.provider_;
    }

    /**
     * @brief compare two clob object references
     * @param a first arg to compare
     * @param b second arg to compare
     * @return true if a != b
     * @return false otherwise
     */
    friend bool operator!=(clob_reference const& a, clob_reference const& b) noexcept {
        return ! (a == b);
    }

    /**
     * @brief appends string representation of the given value.
     * @param out the target output
     * @param value the target value
     * @return the output
     */
    friend std::ostream& operator<<(std::ostream& out, clob_reference const& value) {
        return out << "id:" << value.id_ << "provider:" << value.provider_;
    }

private:
    clob_id_type id_{};
    lob_data_provider provider_{};
};

static_assert(std::is_trivially_copyable_v<clob_reference>);
static_assert(std::is_trivially_destructible_v<clob_reference>);
static_assert(std::alignment_of_v<clob_reference> == 8);
static_assert(sizeof(clob_reference) == 16);

}  // namespace jogasaki
