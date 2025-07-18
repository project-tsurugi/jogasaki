/*
 * Copyright 2018-2025 Project Tsurugi.
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

#include <chrono>
#include <cmath>
#include <cstddef>
#include <decimal.hh>
#include <functional>

#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/time_point.h>

#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_traits.h>

namespace jogasaki::executor {

using kind = meta::field_type_kind;

template <class T>
bool less(T const& x, T const& y) {
    return std::less<T>{}(x, y);
}

template <class T>
bool float_less(T const& x, T const& y) {
    if(std::isnan(y)) {
        return ! std::isnan(x);
    }
    if(std::isnan(x)) {
        return false;
    }
    // +INF/-INF/+0/-0 are handled correctly by std::less
    return std::less<T>{}(x, y);
}

template <>
inline bool less<runtime_t<meta::field_type_kind::float4>>(
    runtime_t<meta::field_type_kind::float4> const& x,
    runtime_t<meta::field_type_kind::float4> const& y
) {
    return float_less(x, y);
}

template <>
inline bool less<runtime_t<meta::field_type_kind::float8>>(
    runtime_t<meta::field_type_kind::float8> const& x,
    runtime_t<meta::field_type_kind::float8> const& y
) {
    return float_less(x, y);
}

template <>
inline bool less<runtime_t<meta::field_type_kind::decimal>>(
    runtime_t<meta::field_type_kind::decimal> const& x,
    runtime_t<meta::field_type_kind::decimal> const& y
) {
    // Decimal can be safely created from triple and comparared without context
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

template <>
inline bool less<runtime_t<meta::field_type_kind::blob>>(
    runtime_t<meta::field_type_kind::blob> const& x,
    runtime_t<meta::field_type_kind::blob> const& y
) {
    // blob is not comparable, but some testcases need equal comparison of blob references
    if (x.provider() != y.provider()) {
        return x.provider() < y.provider();
    }
    return x.object_id() < y.object_id();
}

template <>
inline bool less<runtime_t<meta::field_type_kind::clob>>(
    runtime_t<meta::field_type_kind::clob> const& x,
    runtime_t<meta::field_type_kind::clob> const& y
) {
    // clob is not comparable, but some testcases need equal comparison of blob references
    if (x.provider() != y.provider()) {
        return x.provider() < y.provider();
    }
    return x.object_id() < y.object_id();
}

}
