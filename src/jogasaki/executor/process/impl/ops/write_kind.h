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

#include <vector>

#include <takatori/util/fail.h>
#include <takatori/relation/write.h>

namespace jogasaki::executor::process::impl::ops {

using takatori::util::fail;
namespace relation = takatori::relation;

/**
 * @brief write kind corresponding to takatori::relation::write_kind
 */
enum class write_kind : std::size_t {
    insert = 0,
    update,
    delete_,
    insert_overwrite,
    insert_skip,
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponded string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(write_kind value) noexcept {
    using namespace std::string_view_literals;
    using kind = write_kind;
    switch (value) {
        case kind::insert: return "insert"sv;
        case kind::update: return "update"sv;
        case kind::delete_: return "delete_"sv;
        case kind::insert_overwrite: return "insert_overwrite"sv;
        case kind::insert_skip: return "insert_skip"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, write_kind value) {
    return out << to_string_view(value);
}

constexpr inline write_kind write_kind_from(relation::write_kind kind) noexcept {
    using k = relation::write_kind;
    switch (kind) {
        case k::insert: return write_kind::insert;
        case k::update: return write_kind::update;
        case k::delete_: return write_kind::delete_;
        case k::insert_overwrite: return write_kind::insert_overwrite;
        case k::insert_skip: return write_kind::insert_skip;
    }
    std::abort();
}

}


