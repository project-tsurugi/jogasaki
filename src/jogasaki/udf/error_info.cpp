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

#include "error_info.h"

#include <string>
#include <string_view>

#include <grpcpp/support/status_code_enum.h>
namespace plugin::udf {
using error_code_type = grpc::StatusCode;
error_code_type error_info::code() const noexcept { return code_; }
std::string_view error_info::message() const noexcept { return message_; }
// @see https://github.com/grpc/grpc/blob/master/include/grpcpp/support/status_code_enum.h#L43
std::string_view to_string_view(error_code_type code) noexcept {
    using namespace std::literals;
    switch(code) {
        case grpc::StatusCode::OK: return "OK"sv;
        case grpc::StatusCode::CANCELLED: return "CANCELLED"sv;
        case grpc::StatusCode::UNKNOWN: return "UNKNOWN"sv;
        case grpc::StatusCode::INVALID_ARGUMENT: return "INVALID_ARGUMENT"sv;
        case grpc::StatusCode::DEADLINE_EXCEEDED: return "DEADLINE_EXCEEDED"sv;
        case grpc::StatusCode::NOT_FOUND: return "NOT_FOUND"sv;
        case grpc::StatusCode::ALREADY_EXISTS: return "ALREADY_EXISTS"sv;
        case grpc::StatusCode::PERMISSION_DENIED: return "PERMISSION_DENIED"sv;
        case grpc::StatusCode::RESOURCE_EXHAUSTED: return "RESOURCE_EXHAUSTED"sv;
        case grpc::StatusCode::FAILED_PRECONDITION: return "FAILED_PRECONDITION"sv;
        case grpc::StatusCode::ABORTED: return "ABORTED"sv;
        case grpc::StatusCode::OUT_OF_RANGE: return "OUT_OF_RANGE"sv;
        case grpc::StatusCode::UNIMPLEMENTED: return "UNIMPLEMENTED"sv;
        case grpc::StatusCode::INTERNAL: return "INTERNAL"sv;
        case grpc::StatusCode::UNAVAILABLE: return "UNAVAILABLE"sv;
        case grpc::StatusCode::DATA_LOSS: return "DATA_LOSS"sv;
        case grpc::StatusCode::UNAUTHENTICATED: return "UNAUTHENTICATED"sv;
        default: return "UNKNOWN_CODE"sv;
    }
}
std::string_view to_string_view(plugin::udf::load_status status) noexcept {
    using namespace std::literals;
    switch(status) {
        case load_status::ok: return "ok"sv;
        case load_status::path_not_found: return "path_not_found"sv;
        case load_status::not_regular_file_or_dir: return "not_regular_file_or_dir"sv;
        case load_status::no_shared_objects_found: return "no_shared_objects_found"sv;
        case load_status::dlopen_failed: return "dlopen_failed"sv;
        case load_status::api_symbol_missing: return "api_symbol_missing"sv;
        case load_status::api_init_failed: return "api_init_failed"sv;
        case load_status::factory_symbol_missing: return "factory_symbol_missing"sv;
        case load_status::factory_creation_failed: return "factory_creation_failed"sv;
        default: return "unknown_status"sv;
    }
}

[[nodiscard]] load_status load_result::status() const noexcept { return status_; }
[[nodiscard]] std::string load_result::file() const noexcept { return file_; }
[[nodiscard]] std::string load_result::detail() const noexcept { return detail_; }
void load_result::set_status(load_status s) noexcept { status_ = s; }
void load_result::set_file(std::string f) noexcept { file_ = std::move(f); }
void load_result::set_detail(std::string d) noexcept { detail_ = std::move(d); }

std::ostream& operator<<(std::ostream& out, grpc::StatusCode const& code) { return out << to_string_view(code); }
std::ostream& operator<<(std::ostream& out, load_status const& status) { return out << to_string_view(status); }
}  // namespace plugin::udf
