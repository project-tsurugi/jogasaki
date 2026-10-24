/*
 * Copyright 2026 Project Tsurugi.
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
#include "convert_value.h"
#include <cmath>
#include <limits>
#include <type_traits>
#include <jogasaki/data/field_operations.h>

namespace jogasaki::data {
namespace {
using ordering = value_conversion::ordering;
using result = value_conversion::result;
}
template<class Source, class Target>
static ordering rounding_order(Source value, Target converted) {
    if (static_cast<Source>(converted) < value) {
        return ordering::rounded_down;
    }
    if (static_cast<Source>(converted) > value) {
        return ordering::rounded_up;
    }
    return ordering::exact;
}

template<class Target, class Source>
static result integral_target(Source value, any const& source) {
    if constexpr (std::is_floating_point_v<Source>) {
        // Exclusive upper bound avoids rounding INT64_MAX to the unrepresentable 2^63.
        auto limit = std::ldexp(1.0L, std::numeric_limits<Target>::digits);
        auto truncated = std::trunc(static_cast<long double>(value));
        if (!std::isfinite(value) || truncated < -limit || truncated >= limit) {
            return {ordering::unsupported, source};
        }
    } else if constexpr (sizeof(Target) < sizeof(Source)) {
        if (value < std::numeric_limits<Target>::min()) {
            return {ordering::below_minimum, any{std::in_place_type<Target>, std::numeric_limits<Target>::min()}};
        }
        if (value > std::numeric_limits<Target>::max()) {
            return {ordering::above_maximum, any{std::in_place_type<Target>, std::numeric_limits<Target>::max()}};
        }
    }
    auto converted = static_cast<Target>(value);
    auto order = ordering::exact;
    if constexpr (std::is_floating_point_v<Source>) {
        order = rounding_order(value, converted);
    }
    return {order, any{std::in_place_type<Target>, converted}};
}

template<class Target, class Source>
static result approximate_target(Source value, any const& source) {
    if constexpr (std::is_same_v<Target, float> && std::is_same_v<Source, double>) {
        if (std::isfinite(value) && std::fabs(value) > std::numeric_limits<float>::max()) {
            return {ordering::unsupported, source};
        }
        auto converted = static_cast<Target>(value);
        return {rounding_order(value, converted), any{std::in_place_type<Target>, converted}};
    }
    return {ordering::exact, any{std::in_place_type<Target>, static_cast<Target>(value)}};
}

template<class Target> static result numeric_cast(any const& source) {
    return source.visit([&](auto const& value) -> result {
        using Source = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<Source, std::int32_t> || std::is_same_v<Source, std::int64_t>
                   || std::is_same_v<Source, float> || std::is_same_v<Source, double>) {
            if constexpr (std::is_integral_v<Target>) {
                return integral_target<Target>(value, source);
            } else {
                return approximate_target<Target>(value, source);
            }
        } else {
            return {ordering::unsupported, source};
        }
    });
}
value_conversion value_conversion::prepare(meta::field_type const& target) {
    using k = meta::field_type_kind;
    switch (target.kind()) {
        case k::int4: return {numeric_cast<std::int32_t>, {}};
        case k::int8: return {numeric_cast<std::int64_t>, {}};
        case k::float4: return {numeric_cast<float>, {}};
        case k::float8: return {numeric_cast<double>, {}};
        case k::boolean: case k::int1: case k::int2: case k::character: case k::octet:
        case k::decimal: case k::date: case k::time_of_day: case k::time_point: case k::blob: case k::clob:
            return {nullptr, field_operations::for_type(target)};
        default: return {nullptr, {}};
    }
}
value_conversion::result value_conversion::apply(any const& source) const {
    if (source.error()) {
        return {ordering::unsupported, source};
    }
    if (source.empty()) {
        return {ordering::exact, source};
    }
    if (convert_) {
        return convert_(source);
    }
    return {operations_.accepts(source) ? ordering::exact : ordering::unsupported, source};
}
}
