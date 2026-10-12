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
#include "remote_execution_result.h"

#include <limits>
#include <unordered_set>

#include <takatori/type/primitive.h>

#include <jogasaki/data/small_record_store.h>
#include <jogasaki/proto/distributed_query/remote_execution_block.pb.h>
#include <jogasaki/serializer/value_writer.h>
#include <jogasaki/utils/field_types.h>

#include "remote_record_codec.h"
#include "remote_type_codec.h"

namespace jogasaki::dist {
namespace proto = jogasaki::proto::distributed_query;
namespace {
class buffer_writer {
public:
    bool write(char const* data, std::size_t length) {
        bytes_.append(data, length);
        return true;
    }
    std::string release() { return std::move(bytes_); }

private:
    std::string bytes_{};
};
} // namespace
std::string serialize_remote_execution_result(remote_execution_result const& result) {
    proto::RemoteExecutionResult message{};
    message.set_version(2);
    for (auto const& column : result.columns) {
        auto* out = message.mutable_schema()->add_columns();
        out->set_column_id(column.column_id);
        out->set_type(column.type);
        out->set_nullable(column.nullable);
        out->set_type_payload(column.type_payload.empty() && column.type == "int8()"
                                  ? encode_remote_type(takatori::type::int8{})
                                  : column.type_payload);
    }
    if (!result.encoded_rows.empty()) {
        for (auto const& row : result.encoded_rows) {
            message.add_encoded_rows(row);
        }
    } else {
        for (auto const& row : result.rows) {
            buffer_writer buffer{};
            serializer::value_writer<buffer_writer> out{buffer};
            out.write_row_begin(row.size());
            for (auto const& value : row) {
                if (value) {
                    out.write_int(*value);
                } else {
                    out.write_null();
                }
            }
            message.add_encoded_rows(buffer.release());
        }
    }
    return message.SerializeAsString();
}
std::optional<remote_execution_result> deserialize_remote_execution_result(std::string_view payload,
                                                                           std::string& error) {
    proto::RemoteExecutionResult message{};
    if (payload.size() > std::size_t{64} * 1024U * 1024U ||
        !message.ParseFromArray(payload.data(), static_cast<int>(payload.size()))) {
        error = "invalid RemoteExecutionResult protobuf payload";
        return {};
    }
    if (message.version() != 2 || message.rows_size() != 0) {
        error = "unsupported RemoteExecutionResult version or row encoding";
        return {};
    }
    remote_execution_result result{};
    std::vector<meta::field_type> fields{};
    boost::dynamic_bitset<std::uint64_t> nullable(message.schema().columns_size());
    std::unordered_set<std::uint64_t> ids{};
    bool all_int8 = true;
    for (auto const& column : message.schema().columns()) {
        auto type = decode_remote_type(column.type_payload());
        if (!type || !ids.emplace(column.column_id()).second) {
            error = "invalid remote result schema";
            return {};
        }
        using k = takatori::type::type_kind;
        switch (type->kind()) {
            case k::boolean:
            case k::int4:
            case k::int8:
            case k::float4:
            case k::float8:
            case k::decimal:
            case k::character:
            case k::octet:
            case k::date:
            case k::time_of_day:
            case k::time_point:
                break;
            default:
                error = "unsupported remote result type";
                return {};
        }
        all_int8 = all_int8 && type->kind() == k::int8;
        nullable[fields.size()] = column.nullable();
        fields.emplace_back(utils::type_for(*type));
        result.columns.push_back({column.column_id(), column.type(), column.nullable(), column.type_payload()});
    }
    auto meta = std::make_shared<meta::record_meta>(std::move(fields), std::move(nullable));
    for (auto const& encoded : message.encoded_rows()) {
        data::small_record_store record{meta};
        if (!decode_remote_record(encoded, *meta, record.ref(), error)) {
            return {};
        }
        result.encoded_rows.emplace_back(encoded);
        if (all_int8) {
            std::vector<std::optional<std::int64_t>> row{};
            for (std::size_t i = 0; i < meta->field_count(); ++i) {
                if (meta->nullable(i) && record.ref().is_null(meta->nullity_offset(i))) {
                    row.emplace_back(std::nullopt);
                } else {
                    row.emplace_back(record.ref().get_value<std::int64_t>(meta->value_offset(i)));
                }
            }
            result.rows.emplace_back(std::move(row));
        }
    }
    return result;
}
} // namespace jogasaki::dist
