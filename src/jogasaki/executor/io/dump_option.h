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

#include <cstddef>
#include <cstdlib>
#include <ostream>
#include <string_view>

namespace jogasaki::executor::io {

enum class dump_file_format_kind {
    unspecified,
    parquet,
    arrow,
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponded string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(dump_file_format_kind value) noexcept {
    using namespace std::string_view_literals;
    using kind = dump_file_format_kind;
    switch (value) {
        case kind::unspecified: return "unspecified"sv;
        case kind::parquet: return "parquet"sv;
        case kind::arrow: return "arrow"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, dump_file_format_kind value) {
    return out << to_string_view(value);
}

struct dump_cfg {
    constexpr static std::size_t undefined = static_cast<std::size_t>(-1);

    std::size_t max_records_per_file_{undefined};
    std::size_t max_file_byte_size_{undefined};
    bool keep_files_on_error_{false};

    dump_file_format_kind file_format_{dump_file_format_kind::unspecified};
};

}  // namespace jogasaki::executor::io
