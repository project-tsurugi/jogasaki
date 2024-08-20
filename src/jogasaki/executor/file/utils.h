/*
 * Copyright 2018-2023 Project Tsurugi.
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
#include <cstdint>

#include <jogasaki/executor/file/time_unit_kind.h>
#include <jogasaki/meta/field_type_traits.h>

namespace jogasaki::executor::file {

/**
 * @brief convert time point runtime value to integer represented in the specified time unit
 */
std::int64_t value_in_time_unit(runtime_t<meta::field_type_kind::time_point> v, time_unit_kind kind);

}  // namespace jogasaki::executor::file
