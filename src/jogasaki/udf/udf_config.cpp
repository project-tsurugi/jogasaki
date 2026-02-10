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
#include "udf_config.h"

#include <string>
#include <utility>

namespace plugin::udf {
udf_config::udf_config(bool enabled, std::string endpoint, std::string transport, bool secure)
    : _enabled(enabled), _endpoint(std::move(endpoint)), _transport(std::move(transport)),
      _secure(secure) {}

bool udf_config::enabled() const noexcept { return _enabled; }

std::string const& udf_config::endpoint() const noexcept { return _endpoint; }

std::string const& udf_config::transport() const noexcept { return _transport; }

bool udf_config::secure() const noexcept { return _secure; }
} // namespace plugin::udf