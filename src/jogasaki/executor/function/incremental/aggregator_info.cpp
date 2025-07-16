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
#include "aggregator_info.h"

#include <utility>

#include <jogasaki/executor/function/value_generator.h>

namespace jogasaki::executor::function::incremental {

aggregator_info::aggregator_info(
    aggregator_type aggregator,
    std::size_t arg_count,
    empty_value_generator_type empty_generator
) :
    valid_(true),
    aggregator_(std::move(aggregator)),
    arg_count_(arg_count),
    empty_generator_(std::move(empty_generator))
{}

aggregator_type const& aggregator_info::aggregator() const noexcept {
    return aggregator_;
}

aggregator_info::operator bool() const noexcept {
    return valid_;
}

std::size_t aggregator_info::arg_count() const noexcept {
    return arg_count_;
}

[[nodiscard]] empty_value_generator_type const& aggregator_info::empty_value_generator() const noexcept {
    return empty_generator_;
}

}
