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

#include <cstddef>
#include <cstdlib>
#include <ostream>
#include <string_view>

#include <jogasaki/executor/file/time_unit_kind.h>

namespace jogasaki::executor::io {

enum class dump_file_format_kind {
    unspecified,
    parquet,
    arrow,
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponding string representation
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

struct dump_config {

    /**
     * @brief max number of records written in a result file
     */
    std::size_t max_records_per_file_{};

    /**
     * @brief max number of bytes written in a result file (not implemented yet)
     */
    std::size_t max_file_byte_size_{};

    /**
     * @brief whether the results files are kept or removed when error occurs
     */
    bool keep_files_on_error_{false};

    /**
     * @brief dump file format
     */
    dump_file_format_kind file_format_{dump_file_format_kind::unspecified};

    /**
     * @brief max number of records written in a row group (record batch)
     * @details 0 means undefined
     */
    std::int64_t record_batch_size_{};

    /**
     * @brief max number of bytes written in a row group (record batch)
     * @details 0 means undefined
     */
    std::int64_t record_batch_in_bytes_{};

    /**
     * @brief whether arrow file uses fixed size binary for char data types
     */
    bool arrow_use_fixed_size_binary_for_char_{};

    /**
     * @brief time unit used when timestamp is dumped
     */
    file::time_unit_kind time_unit_kind_{file::time_unit_kind::unspecified};
};

}  // namespace jogasaki::executor::io
