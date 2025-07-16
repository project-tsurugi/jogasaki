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
#include "utils.h"

#include <cstddef>
#include <cstdint>

#include <takatori/datetime/time_point.h>

#include <jogasaki/executor/file/time_unit_kind.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_traits.h>

namespace jogasaki::executor::file {

std::int64_t value_in_time_unit(runtime_t<meta::field_type_kind::time_point> v, time_unit_kind kind) {
    auto secs = static_cast<std::int64_t>(v.seconds_since_epoch().count());
    auto subsecs_ns = static_cast<std::int64_t>(v.subsecond().count());
    std::int64_t ret{};
    using k = time_unit_kind;
    switch(kind) {
        case k::second: ret = secs; break;
        case k::millisecond: ret = secs*1000 + subsecs_ns/1000/1000; break;
        case k::microsecond: ret = secs*1000*1000 + subsecs_ns/1000; break;
        case k::nanosecond: [[fallthrough]];
        default: ret = secs*1000*1000*1000 + subsecs_ns; break;
    }
    return ret;
}

}  // namespace jogasaki::executor::file
