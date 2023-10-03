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
#include <functional>

#include <jogasaki/meta/record_meta.h>
#include <jogasaki/accessor/record_ref.h>
#include "compare_info.h"

namespace jogasaki::executor {

using kind = meta::field_type_kind;

template <class T>
bool less(T const& x, T const& y) {
    return std::less<T>{}(x, y);
}

template <>
inline bool less<runtime_t<meta::field_type_kind::decimal>>(
    runtime_t<meta::field_type_kind::decimal> const& x,
    runtime_t<meta::field_type_kind::decimal> const& y
) {
    // TODO context
    return static_cast<decimal::Decimal>(x) < static_cast<decimal::Decimal>(y);
}

template <>
inline bool less<runtime_t<meta::field_type_kind::date>>(
    runtime_t<meta::field_type_kind::date> const& x,
    runtime_t<meta::field_type_kind::date> const& y
) {
    return x.days_since_epoch() < y.days_since_epoch();
}

template <>
inline bool less<runtime_t<meta::field_type_kind::time_of_day>>(
    runtime_t<meta::field_type_kind::time_of_day> const& x,
    runtime_t<meta::field_type_kind::time_of_day> const& y
) {
    return x.time_since_epoch().count() < y.time_since_epoch().count();
}

template <>
inline bool less<runtime_t<meta::field_type_kind::time_point>>(
    runtime_t<meta::field_type_kind::time_point> const& x,
    runtime_t<meta::field_type_kind::time_point> const& y
) {
    if(x.seconds_since_epoch().count() != y.seconds_since_epoch().count()) {
        return x.seconds_since_epoch().count() < y.seconds_since_epoch().count();
    }
    return x.subsecond() < y.subsecond();
}
}
