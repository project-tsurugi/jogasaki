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
#include <string>
#include <string_view>

#include <takatori/type/data.h>
#include <takatori/value/data.h>

namespace jogasaki::dist {
[[nodiscard]] std::string encode_remote_type(takatori::type::data const& data);
[[nodiscard]] std::shared_ptr<takatori::type::data const> decode_remote_type(std::string_view payload);
[[nodiscard]] std::string encode_remote_literal(takatori::value::data const& data);
[[nodiscard]] std::unique_ptr<takatori::value::data> decode_remote_literal(std::string_view payload,
                                                                           takatori::type::data const& type);
} // namespace jogasaki::dist
