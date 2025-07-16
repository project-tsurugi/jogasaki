/*
 * Copyright 2018-2024 Project Tsurugi.
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
#include "scalar_function_info.h"

#include <utility>

#include <jogasaki/executor/function/scalar_function_kind.h>
// #include <jogasaki/executor/function/value_generator.h>

namespace jogasaki::executor::function {

scalar_function_info::scalar_function_info(
    scalar_function_kind kind,
    // empty_value_generator_type empty_generator,
    scalar_function_type function_body,
    std::size_t arg_count
) :
    kind_(kind),
    // empty_generator_(std::move(empty_generator)),
    function_body_(std::move(function_body)),
    arg_count_(arg_count)
{}

// empty_value_generator_type const& scalar_function_info::empty_value_generator() const noexcept {
//     return empty_generator_;
// }

scalar_function_type const& scalar_function_info::function_body() const noexcept {
    return function_body_;
}

std::size_t scalar_function_info::arg_count() const noexcept {
    return arg_count_;
}

}  // namespace jogasaki::executor::function
