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
std::string error_info::code_string() const noexcept {
    switch(code_) {
        case grpc::StatusCode::OK: return "OK";
        case grpc::StatusCode::CANCELLED: return "CANCELLED";
        case grpc::StatusCode::UNKNOWN: return "UNKNOWN";
        case grpc::StatusCode::INVALID_ARGUMENT: return "INVALID_ARGUMENT";
        case grpc::StatusCode::DEADLINE_EXCEEDED: return "DEADLINE_EXCEEDED";
        case grpc::StatusCode::NOT_FOUND: return "NOT_FOUND";
        case grpc::StatusCode::ALREADY_EXISTS: return "ALREADY_EXISTS";
        case grpc::StatusCode::PERMISSION_DENIED: return "PERMISSION_DENIED";
        case grpc::StatusCode::RESOURCE_EXHAUSTED: return "RESOURCE_EXHAUSTED";
        case grpc::StatusCode::FAILED_PRECONDITION: return "FAILED_PRECONDITION";
        case grpc::StatusCode::ABORTED: return "ABORTED";
        case grpc::StatusCode::OUT_OF_RANGE: return "OUT_OF_RANGE";
        case grpc::StatusCode::UNIMPLEMENTED: return "UNIMPLEMENTED";
        case grpc::StatusCode::INTERNAL: return "INTERNAL";
        case grpc::StatusCode::UNAVAILABLE: return "UNAVAILABLE";
        case grpc::StatusCode::DATA_LOSS: return "DATA_LOSS";
        case grpc::StatusCode::UNAUTHENTICATED: return "UNAUTHENTICATED";
        default: return "UNKNOWN_CODE";
    }
}

[[nodiscard]] load_status load_result::status() const noexcept { return status_; }
[[nodiscard]] std::string load_result::file() const noexcept { return file_; }
[[nodiscard]] std::string load_result::detail() const noexcept { return detail_; }
void load_result::set_status(load_status s) noexcept { status_ = s; }
void load_result::set_file(std::string f) noexcept { file_ = std::move(f); }
void load_result::set_detail(std::string d) noexcept { detail_ = std::move(d); }

[[nodiscard]] std::string load_result::status_string() const noexcept {
    switch(status_) {
        case load_status::ok: return "OK";
        case load_status::path_not_found: return "PathNotFound";
        case load_status::not_regular_file_or_dir: return "NotRegularFileOrDir";
        case load_status::no_shared_objects_found: return "NoSharedObjectsFound";
        case load_status::dlopen_failed: return "DLOpenFailed";
        case load_status::api_symbol_missing: return "ApiSymbolMissing";
        case load_status::api_init_failed: return "ApiInitFailed";
        case load_status::factory_symbol_missing: return "FactorySymbolMissing";
        case load_status::factory_creation_failed: return "FactoryCreationFailed";
        default: return "UnknownStatus";
    }
}
}  // namespace plugin::udf
