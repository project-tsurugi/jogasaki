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

#include <cstddef>
#include <optional>
#include <string>
#include <vector>

namespace plugin::udf {

struct udf_server_config {
    std::string endpoint{};
    bool secure{false};
    std::string tsurugi_endpoint{};
};

class udf_config {
  public:
    udf_config() = default;
    udf_config(udf_config const&) = default;
    udf_config(udf_config&&) noexcept = default;
    udf_config(bool enabled, std::string endpoint, std::string transport, bool secure,
        std::optional<std::string> grpc_server_endpoint = std::nullopt,
        std::optional<std::size_t> timeout = std::nullopt);
    udf_config(bool enabled, std::vector<udf_server_config> servers, std::string transport,
        std::optional<std::string> grpc_server_endpoint = std::nullopt,
        std::optional<std::size_t> timeout = std::nullopt);
    udf_config& operator=(udf_config const&) = default;
    udf_config& operator=(udf_config&&) noexcept = default;
    ~udf_config() = default;

    // Accessors
    [[nodiscard]] bool enabled() const noexcept;
    [[nodiscard]] std::vector<udf_server_config> const& servers() const noexcept;

    [[nodiscard]] std::string const& endpoint() const noexcept;
    [[nodiscard]] std::string const& transport() const noexcept;
    [[nodiscard]] bool secure() const noexcept;
    [[nodiscard]] std::optional<std::string> const& grpc_server_endpoint() const noexcept;
    [[nodiscard]] std::optional<std::size_t> const& timeout() const noexcept;

  private:
    bool _enabled{true};
    std::vector<udf_server_config> _servers{{"dns:///localhost:50051", false, ""}};
    std::string _transport{"stream"};
    std::optional<std::string> _grpc_server_endpoint{};
    std::optional<std::size_t> _timeout{std::nullopt};
};

} // namespace plugin::udf
