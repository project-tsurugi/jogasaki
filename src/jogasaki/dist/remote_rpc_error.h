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

#include <string>
#include <string_view>
#include <grpcpp/support/status.h>

#include <jogasaki/error_code.h>
#include <jogasaki/status.h>

namespace jogasaki::dist {

struct remote_rpc_error {
    status result;
    error_code code;
    std::string_view reason;
};

// Shared by ExecuteRemoteProcess and GetRemoteCatalog. SQL details are handled before this mapping.
[[nodiscard]] inline remote_rpc_error classify_remote_rpc_error(grpc::StatusCode code) noexcept {
    switch (code) {
        case grpc::StatusCode::OK:
            return {status::ok, error_code::none, {}};
        case grpc::StatusCode::CANCELLED:
            return {status::err_io_error, error_code::sql_execution_exception, "was cancelled"};
        case grpc::StatusCode::DEADLINE_EXCEEDED:
            return {status::err_time_out, error_code::sql_request_timeout_exception, "timed out"};
        case grpc::StatusCode::RESOURCE_EXHAUSTED:
            return {status::err_resource_limit_reached, error_code::sql_execution_exception, "failed: resource exhausted"};
        case grpc::StatusCode::UNAVAILABLE:
            return {status::err_io_error, error_code::sql_execution_exception, "failed: remote server unavailable"};
        case grpc::StatusCode::INVALID_ARGUMENT:
            return {status::err_invalid_argument, error_code::sql_execution_exception, "failed: invalid RPC request"};
        case grpc::StatusCode::DATA_LOSS:
            return {status::err_invalid_state, error_code::internal_exception, "failed: RPC reported data loss"};
        case grpc::StatusCode::UNIMPLEMENTED:
            return {status::err_unsupported, error_code::unsupported_runtime_feature_exception, "failed: unsupported feature"};
        case grpc::StatusCode::NOT_FOUND:
            return {status::err_not_found, error_code::sql_execution_exception, "failed: remote RPC target not found"};
        case grpc::StatusCode::ALREADY_EXISTS:
            return {status::err_unknown, error_code::sql_execution_exception, "failed: remote RPC target already exists"};
        case grpc::StatusCode::PERMISSION_DENIED:
            return {status::err_unknown, error_code::sql_execution_exception, "failed: remote RPC permission denied"};
        case grpc::StatusCode::FAILED_PRECONDITION:
            return {status::err_unknown, error_code::sql_execution_exception, "failed: remote precondition not satisfied"};
        case grpc::StatusCode::ABORTED:
            return {status::err_unknown, error_code::sql_execution_exception, "was aborted"};
        case grpc::StatusCode::OUT_OF_RANGE:
            return {status::err_unknown, error_code::sql_execution_exception, "failed: RPC reported an out-of-range error"};
        case grpc::StatusCode::INTERNAL:
            return {status::err_unknown, error_code::internal_exception, "failed: RPC internal error"};
        case grpc::StatusCode::UNAUTHENTICATED:
            return {status::err_unknown, error_code::sql_execution_exception, "failed: remote RPC authentication failed"};
        default:
            return {status::err_unknown, error_code::sql_execution_exception, "failed: unclassified RPC error"};
    }
}

[[nodiscard]] inline std::string remote_rpc_message(std::string_view operation, remote_rpc_error const& failure) {
    auto result = std::string{"[distributed-query] "} + std::string{operation};
    result += " ";
    result += failure.reason;
    return result;
}

} // namespace jogasaki::dist
