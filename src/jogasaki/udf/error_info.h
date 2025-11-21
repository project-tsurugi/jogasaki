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
#include <string>
#include <string_view>

#include "enum_types.h"

#include <grpcpp/support/status_code_enum.h>
namespace plugin::udf {

// @see https://protobuf.dev/programming-guides/proto3/
class error_info {
public:

    using error_code_type = grpc::StatusCode;
    ~error_info() = default;
    explicit error_info(error_code_type code, std::string msg) : code_(code), message_(std::move(msg)) {}
    error_info(error_info const&) = default;
    error_info(error_info&&) noexcept = default;
    error_info& operator=(error_info const&) = default;
    error_info& operator=(error_info&&) noexcept = default;
    [[nodiscard]] error_code_type code() const noexcept;
    [[nodiscard]] std::string_view message() const noexcept;

private:

    error_code_type code_{grpc::StatusCode::OK};
    std::string message_{};
};
class load_result {
public:

    load_result(load_status s, std::string f, std::string d) noexcept :
        status_(s),
        file_(std::move(f)),
        detail_(std::move(d)) {}
    load_result() = delete;
    ~load_result() = default;
    load_result(load_result const&) = default;
    load_result(load_result&&) noexcept = default;
    load_result& operator=(load_result const&) = default;
    load_result& operator=(load_result&&) noexcept = default;
    [[nodiscard]] load_status status() const noexcept;
    [[nodiscard]] std::string file() const noexcept;
    [[nodiscard]] std::string detail() const noexcept;
    void set_status(load_status s) noexcept;
    void set_file(std::string f) noexcept;
    void set_detail(std::string d) noexcept;

private:

    load_status status_{load_status::ok};
    std::string file_{};
    std::string detail_{};
};

[[nodiscard]] std::string_view to_string_view(grpc::StatusCode code) noexcept;
[[nodiscard]] std::string_view to_string_view(load_status status) noexcept;
std::ostream& operator<<(std::ostream& out, grpc::StatusCode const& code);
std::ostream& operator<<(std::ostream& out, load_status const& status);
}  // namespace plugin::udf
