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

#include <algorithm>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <takatori/type/primitive.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/dist/remote_execution_result.h>
#include <jogasaki/dist/remote_record_codec.h>
#include <jogasaki/dist/remote_type_codec.h>
#include <jogasaki/serializer/value_writer.h>
#include <jogasaki/meta/record_meta.h>

namespace jogasaki::test_utils {

// Convenience view for tests that use BIGINT rows. Production results only carry encoded rows.
struct int8_remote_result {
    std::vector<dist::remote_result_column> columns{};
    std::vector<std::vector<std::optional<std::int64_t>>> rows{};
    std::vector<std::string> encoded_rows{};
};

namespace detail {
class remote_result_buffer {
public:
    bool write(char const* data, std::size_t length) {
        bytes_.append(data, length);
        return true;
    }
    std::string release() { return std::move(bytes_); }
private:
    std::string bytes_{};
};
} // namespace detail

inline std::string serialize_int8_remote_result(int8_remote_result const& source) {
    dist::remote_execution_result result{source.columns, source.encoded_rows};
    if (result.encoded_rows.empty()) {
        for (auto const& row : source.rows) {
            detail::remote_result_buffer buffer{};
            serializer::value_writer<detail::remote_result_buffer> out{buffer};
            out.write_row_begin(row.size());
            for (auto const& value : row) {
                if (value) out.write_int(*value);
                else out.write_null();
            }
            result.encoded_rows.emplace_back(buffer.release());
        }
    }
    return dist::serialize_remote_execution_result(result);
}

inline std::optional<int8_remote_result> deserialize_int8_remote_result(
    std::string_view payload, std::string& error) {
    auto decoded = dist::deserialize_remote_execution_result(payload, error);
    if (!decoded) return {};
    int8_remote_result result{std::move(decoded->columns), {}, std::move(decoded->encoded_rows)};
    auto const all_int8 = std::all_of(result.columns.begin(), result.columns.end(), [](auto const& column) {
        auto type = dist::decode_remote_type(column.type_payload);
        return type && type->kind() == takatori::type::type_kind::int8;
    });
    if (!all_int8) {
        error = "BIGINT test view requires every result column to be int8";
        return {};
    }
    std::vector<meta::field_type> fields(result.columns.size(), meta::field_type{meta::field_enum_tag<meta::field_type_kind::int8>});
    boost::dynamic_bitset<std::uint64_t> nullable(result.columns.size());
    for (std::size_t i = 0; i < result.columns.size(); ++i) nullable[i] = result.columns[i].nullable;
    auto meta = std::make_shared<meta::record_meta>(std::move(fields), std::move(nullable));
    for (auto const& encoded : result.encoded_rows) {
        data::small_record_store record{meta};
        if (!dist::decode_remote_record(encoded, *meta, record.ref(), error)) return {};
        std::vector<std::optional<std::int64_t>> row{};
        for (std::size_t i = 0; i < meta->field_count(); ++i) {
            if (meta->nullable(i) && record.ref().is_null(meta->nullity_offset(i))) row.emplace_back(std::nullopt);
            else row.emplace_back(record.ref().get_value<std::int64_t>(meta->value_offset(i)));
        }
        result.rows.emplace_back(std::move(row));
    }
    return result;
}
} // namespace jogasaki::test_utils
