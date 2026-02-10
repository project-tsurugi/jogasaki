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

#include <string>

namespace plugin::udf {

class udf_config {
public:

    udf_config() = default;
    udf_config(udf_config const&) = default;
    udf_config(udf_config&&) noexcept = default;
    udf_config(bool enabled, std::string endpoint, std::string transport, bool secure);
    udf_config& operator=(udf_config const&) = default;
    udf_config& operator=(udf_config&&) noexcept = default;
    ~udf_config() = default;

    // Accessors
    [[nodiscard]] bool enabled() const noexcept;
    [[nodiscard]] std::string const& endpoint() const noexcept;
    [[nodiscard]] std::string const& transport() const noexcept;
    [[nodiscard]] bool secure() const noexcept;

private:

    bool _enabled{true};
    std::string _endpoint{"dns:///localhost:50051"};
    std::string _transport{"stream"};
    bool _secure{false};
};

}  // namespace plugin::udf
