/*
 * Copyright 2018-2020 tsurugi project.
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
#include "aggregate_function_info.h"

#include <vector>

namespace jogasaki::executor::function {

aggregate_function_info::aggregate_function_info(
    aggregate_function_kind kind,
    aggregator_type aggregator,
    std::size_t arg_count
) :
    kind_(kind),
    aggregator_(std::move(aggregator)),
    arg_count_(arg_count)
{}


aggregator_type const& aggregate_function_info::aggregator() const noexcept {
    return aggregator_;
}

std::size_t aggregate_function_info::arg_count() const noexcept {
    return arg_count_;
}
}
