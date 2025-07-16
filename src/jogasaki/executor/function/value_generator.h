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
#pragma once

#include <functional>
#include <memory>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/executor/function/field_locator.h>

namespace jogasaki::executor::function {

/**
 * @brief definition of aggregator function type for empty input
 */
using empty_value_generator_type = std::function<void (
    accessor::record_ref,
    field_locator const&
)>;

/**
 * @brief zero value generator function for empty input aggregation
 * @param target the target record_ref where result value is written
 * @param target_loc target field locator
 * @note this generator can be used only for the aggregation whose return type is numeric
 */
void zero_generator(
    accessor::record_ref target,
    field_locator const& target_loc
);

/**
 * @brief null value generator function for empty input aggregation
 * @param target the target record_ref where result value is written
 * @param target_loc target field locator
 */
void null_generator(
    accessor::record_ref target,
    field_locator const& target_loc
);

}
