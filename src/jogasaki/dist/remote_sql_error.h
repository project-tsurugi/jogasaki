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

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <google/protobuf/any.pb.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/wire_format_lite.h>

#include <jogasaki/api/impl/map_error_code.h>
#include <jogasaki/error/error_info.h>
#include <jogasaki/proto/sql/response.pb.h>

namespace jogasaki::dist {
inline std::string serialize_remote_sql_error(error::error_info const& info) {
    proto::sql::response::Error out{};
    out.set_code(api::impl::map_error(info.code()));
    out.set_detail(std::string{info.message()});
    out.set_supplemental_text(std::string{info.supplemental_text()});
    return out.SerializeAsString();
}
// The SQL wire contract carries a public error code, not the worker's internal status.
inline std::shared_ptr<error::error_info> restore_remote_sql_error(proto::sql::response::Error const& remote) {
    auto code = api::impl::map_error(remote.code());
    auto st = status::err_unknown;
    switch (code) {
        case error_code::evaluation_exception:
        case error_code::value_evaluation_exception:
        case error_code::scalar_subquery_evaluation_exception:
            st = status::err_expression_evaluation_failure;
            break;
        case error_code::value_out_of_range_exception:
        case error_code::value_too_long_exception:
        case error_code::invalid_decimal_value_exception:
        case error_code::invalid_runtime_value_exception:
            st = status::err_invalid_runtime_value;
            break;
        case error_code::target_not_found_exception:
            st = status::err_not_found;
            break;
        case error_code::unsupported_runtime_feature_exception:
            st = status::err_unsupported;
            break;
        case error_code::sql_limit_reached_exception:
        case error_code::transaction_exceeded_limit_exception:
            st = status::err_resource_limit_reached;
            break;
        case error_code::sql_request_timeout_exception:
            st = status::err_time_out;
            break;
        case error_code::data_corruption_exception:
        case error_code::secondary_index_corruption_exception:
            st = status::err_data_corruption;
            break;
        case error_code::inactive_transaction_exception:
            st = status::err_inactive_transaction;
            break;
        case error_code::cc_exception:
        case error_code::occ_exception:
        case error_code::occ_read_exception:
        case error_code::occ_write_exception:
        case error_code::ltx_exception:
        case error_code::ltx_read_exception:
        case error_code::ltx_write_exception:
        case error_code::rtx_exception:
            st = status::err_serialization_failure;
            break;
        default:
            break;
    }
    return std::make_shared<error::error_info>(code, remote.detail(), st, remote.supplemental_text());
}
namespace detail {
/** @brief Decode the status envelope while rejecting duplicate code, message, and SQL detail fields. */
class remote_sql_status_decoder {
public:
    std::optional<proto::sql::response::Error> decode(std::string_view bytes, int outer_code,
                                                      std::string_view outer_message, bool& protocol_error) {
        protocol_error = false;
        if (bytes.empty() || bytes.size() > std::size_t{64} * 1024U * 1024U) {
            return {};
        }
        google::protobuf::io::ArrayInputStream array{bytes.data(), static_cast<int>(bytes.size())};
        google::protobuf::io::CodedInputStream in{&array};
        for (std::uint32_t tag{}; (tag = in.ReadTag()) != 0;) {
            if (!read_field(in, tag)) {
                return {};
            }
        }
        if (!in.ConsumedEntireMessage() || !has_code_) {
            return {};
        }
        if (status_code_ != static_cast<std::uint32_t>(outer_code) || status_message_ != outer_message) {
            protocol_error = true;
            return {};
        }
        if (status_code_ != 2) {
            return {};
        }
        return result_;
    }

private:
    using wire = google::protobuf::internal::WireFormatLite;
    bool read_detail(google::protobuf::io::CodedInputStream& in) {
        std::string payload{};
        if (!wire::ReadString(&in, &payload)) {
            return false;
        }
        google::protobuf::Any detail{};
        if (!detail.ParseFromString(payload)) {
            return false;
        }
        if (detail.type_url() != "type.googleapis.com/jogasaki.proto.sql.response.Error") {
            return true;
        }
        if (result_) {
            return false;
        }
        result_.emplace();
        return result_->ParseFromString(detail.value()) && proto::sql::error::Code_IsValid(result_->code()) &&
               result_->code() != proto::sql::error::CODE_UNSPECIFIED;
    }
    bool read_field(google::protobuf::io::CodedInputStream& in, std::uint32_t tag) {
        switch (tag) {
            case 8:
                if (has_code_ || !in.ReadVarint32(&status_code_)) {
                    return false;
                }
                has_code_ = true;
                return true;
            case 18:
                if (has_message_ || !wire::ReadString(&in, &status_message_)) {
                    return false;
                }
                has_message_ = true;
                return true;
            case 26:
                return read_detail(in);
            default:
                return wire::SkipField(&in, tag);
        }
    }
    std::optional<proto::sql::response::Error> result_{};
    std::uint32_t status_code_{};
    bool has_code_{};
    bool has_message_{};
    std::string status_message_{};
};
} // namespace detail
// Decode google.rpc.Status without coupling the SQL engine to Google RPC generated types.
inline std::optional<proto::sql::response::Error> deserialize_remote_sql_error(std::string_view bytes, int outer_code,
                                                                               std::string_view outer_message,
                                                                               bool& protocol_error) {
    return detail::remote_sql_status_decoder{}.decode(bytes, outer_code, outer_message, protocol_error);
}
} // namespace jogasaki::dist
