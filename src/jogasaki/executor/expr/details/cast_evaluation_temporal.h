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
#include <string_view>

#include <jogasaki/data/any.h>
#include <jogasaki/executor/expr/evaluator_context.h>

namespace jogasaki::executor::expr::details {

using any = jogasaki::data::any;

/// following functions are private, left for testing

namespace from_character {

any to_date(std::string_view s, evaluator_context& ctx);
any to_time_of_day(std::string_view s, evaluator_context& ctx);
any to_time_point(std::string_view s, bool with_time_zone, evaluator_context& ctx);

}  // namespace from_character

}  // namespace jogasaki::executor::expr::details
