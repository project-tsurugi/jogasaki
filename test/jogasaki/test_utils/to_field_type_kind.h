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
#pragma once

#include <cstddef>

#include <yugawara/compiled_info.h>
#include <takatori/type/type_kind.h>
#include <takatori/descriptor/variable.h>
#include <takatori/type/boolean.h>
#include <takatori/type/int.h>
#include <takatori/type/float.h>
#include <takatori/type/character.h>
#include <takatori/type/decimal.h>
#include <takatori/type/bit.h>
#include <takatori/type/time_of_day.h>
#include <takatori/type/datetime_interval.h>

#include <jogasaki/constants.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>

namespace jogasaki::utils {

using ::takatori::util::fail;

template <class T>
[[nodiscard]] constexpr meta::field_type_kind to_field_type_kind() {
    using k = meta::field_type_kind;
    if constexpr (std::is_same_v<T, takatori::type::boolean>) {
        return k::boolean;
    } else if constexpr (std::is_same_v<T, takatori::type::int1>) {
        return k::int1;
    } else if constexpr (std::is_same_v<T, takatori::type::int2>) {
        return k::int2;
    } else if constexpr (std::is_same_v<T, takatori::type::int4>) {
        return k::int4;
    } else if constexpr (std::is_same_v<T, takatori::type::int8>) {
        return k::int8;
    } else if constexpr (std::is_same_v<T, takatori::type::float4>) {
        return k::float4;
    } else if constexpr (std::is_same_v<T, takatori::type::float8>) {
        return k::float8;
    } else if constexpr (std::is_same_v<T, takatori::type::character>) {
        return k::character;
    } else if constexpr (std::is_same_v<T, takatori::type::decimal>) {
        return k::decimal;
    } else if constexpr (std::is_same_v<T, takatori::type::bit>) {
        return k::bit;
    } else if constexpr (std::is_same_v<T, takatori::type::time_of_day>) {
        return k::time_of_day;
    } else if constexpr (std::is_same_v<T, takatori::type::datetime_interval>) {
        return k::time_interval;
    } else {
        fail();
    }
}

}

