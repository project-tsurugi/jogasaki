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

#include <takatori/value/int.h>
#include <takatori/value/float.h>
#include <takatori/value/character.h>
#include <takatori/value/decimal.h>
#include <takatori/value/date.h>
#include <takatori/value/time_of_day.h>
#include <takatori/value/time_point.h>
#include <takatori/type/int.h>
#include <takatori/type/float.h>
#include <takatori/type/character.h>
#include <takatori/type/decimal.h>
#include <takatori/type/date.h>
#include <takatori/type/time_of_day.h>
#include <takatori/type/time_point.h>
#include <takatori/util/downcast.h>
#include <takatori/util/fail.h>

#include <jogasaki/data/any.h>

namespace jogasaki::utils {

using takatori::util::unsafe_downcast;
using takatori::util::fail;

template<class T>
inline static typename T::view_type value_of(
    takatori::value::data const& arg
) {
    if constexpr (std::is_same_v<T, takatori::value::boolean>) {  //NOLINT
        return unsafe_downcast<takatori::value::boolean>(arg).get();
    } else if constexpr (std::is_same_v<T, takatori::value::int4> || std::is_same_v<T, takatori::value::int8>) { //NOLINT
        if (arg.kind() == takatori::value::value_kind::int4) {
            return unsafe_downcast<takatori::value::int4>(arg).get();
        }
        if (arg.kind() == takatori::value::value_kind::int8) {
            return unsafe_downcast<takatori::value::int8>(arg).get();
        }
        fail();
    } else if constexpr (std::is_same_v<T, takatori::value::float4> || std::is_same_v<T, takatori::value::float8>) { //NOLINT
        if (arg.kind() == takatori::value::value_kind::float4) {
            return unsafe_downcast<takatori::value::float4>(arg).get();
        }
        if (arg.kind() == takatori::value::value_kind::float8) {
            return unsafe_downcast<takatori::value::float8>(arg).get();
        }
        fail();
    } else if constexpr (std::is_same_v<T, takatori::value::character>) { //NOLINT
        return unsafe_downcast<takatori::value::character>(arg).get();
    } else if constexpr (std::is_same_v<T, takatori::value::decimal>) { //NOLINT
        return unsafe_downcast<takatori::value::decimal>(arg).get();
    } else if constexpr (std::is_same_v<T, takatori::value::date>) { //NOLINT
        return unsafe_downcast<takatori::value::date>(arg).get();
    } else if constexpr (std::is_same_v<T, takatori::value::time_of_day>) { //NOLINT
        return unsafe_downcast<takatori::value::time_of_day>(arg).get();
    } else if constexpr (std::is_same_v<T, takatori::value::time_point>) { //NOLINT
        return unsafe_downcast<takatori::value::time_point>(arg).get();
    } else { //NOLINT
        throw std::domain_error("inconsistent value type");
    }
}

data::any as_any(
    takatori::value::data const& arg,
    takatori::type::data const& type,
    memory::paged_memory_resource* resource
);

}

