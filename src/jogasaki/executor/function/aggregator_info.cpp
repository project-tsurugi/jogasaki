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
#include "aggregator_info.h"

#include <takatori/util/fail.h>

namespace jogasaki::executor::function {

using takatori::util::sequence_view;
using takatori::util::enum_tag_t;


aggregator_info::aggregator_info(aggregator_type aggregator) :
    valid_(true),
    aggregator_(std::move(aggregator))
{}

aggregator_type const &aggregator_info::aggregator() const noexcept {
    return aggregator_;
}

aggregator_info::operator bool() const noexcept {
    return valid_;
}
}
