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

#include <cstdint>
#include <grpcpp/support/status_code_enum.h>
#include <string>
#include <string_view>
namespace plugin::udf {

// @see https://protobuf.dev/programming-guides/proto3/
class error_info {
  public:
    using error_code_type = grpc::StatusCode;
    ~error_info()         = default;
    explicit error_info(error_code_type code, std::string msg)
        : code_(code), message_(std::move(msg)) {}
    error_info(const error_info&)                = default;
    error_info(error_info&&) noexcept            = default;
    error_info& operator=(const error_info&)     = default;
    error_info& operator=(error_info&&) noexcept = default;
    [[nodiscard]] error_code_type code() const noexcept;
    [[nodiscard]] std::string_view message() const noexcept;
    [[nodiscard]] std::string code_string() const noexcept;

  private:
    error_code_type code_{grpc::StatusCode::OK};
    std::string message_{};
};

} // namespace plugin::udf
