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
#include "convert_any.h"

#include <cstdint>
#include <utility>

#include <takatori/datetime/date.h>
#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/time_point.h>
#include <takatori/decimal/triple.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/data/any.h>
#include <jogasaki/meta/field_type_kind.h>

namespace jogasaki::utils {

using data::any;

bool convert_any(any& a, meta::field_type const& type) {
    //TODO validation about type compatibility is not strict for now
    if (! a) return true;
    using k = meta::field_type_kind;
    switch(type.kind()) {
        case k::int4: {
            switch(a.type_index()) {
                case any::index<std::int32_t>: break;
                case any::index<std::int64_t>: a = any{std::in_place_type<std::int32_t>, a.to<std::int64_t>()}; break;
                case any::index<float>: a = any{std::in_place_type<std::int32_t>, a.to<float>()}; break;
                case any::index<double>: a = any{std::in_place_type<std::int32_t>, a.to<double>()}; break;
                default: return false;
            }
            break;
        }
        case k::int8: {
            switch(a.type_index()) {
                case any::index<std::int32_t>: a = any{std::in_place_type<std::int64_t>, a.to<std::int32_t>()}; break;
                case any::index<std::int64_t>: break;
                case any::index<float>: a = any{std::in_place_type<std::int64_t>, a.to<float>()}; break;
                case any::index<double>: a = any{std::in_place_type<std::int64_t>, a.to<double>()}; break;
                default: return false;
            }
            break;
        }
        case k::float4: {
            switch(a.type_index()) {
                case any::index<std::int32_t>: a = any{std::in_place_type<float>, a.to<std::int32_t>()}; break;
                case any::index<std::int64_t>: a = any{std::in_place_type<float>, a.to<std::int64_t>()}; break;
                case any::index<float>: break;
                case any::index<double>: a = any{std::in_place_type<float>, a.to<double>()}; break;
                default: return false;
            }
            break;
        }
        case k::float8: {
            switch(a.type_index()) {
                case any::index<std::int32_t>: a = any{std::in_place_type<double>, a.to<std::int32_t>()}; break;
                case any::index<std::int64_t>: a = any{std::in_place_type<double>, a.to<std::int64_t>()}; break;
                case any::index<float>: a = any{std::in_place_type<double>, a.to<float>()}; break;
                case any::index<double>: break;
                default: return false;
            }
            break;
        }
        case k::character: {
            switch(a.type_index()) {
                case any::index<accessor::text>: break;
                default: return false;
            }
            break;
        }
        case k::octet: {
            switch(a.type_index()) {
                case any::index<accessor::binary>: break;
                default: return false;
            }
            break;
        }
        case k::decimal: {
            switch(a.type_index()) {
                case any::index<takatori::decimal::triple>: break;
                default: return false;
            }
            break;
        }
        case k::date: {
            switch(a.type_index()) {
                case any::index<takatori::datetime::date>: break;
                default: return false;
            }
            break;
        }
        case k::time_of_day: {
            switch(a.type_index()) {
                case any::index<takatori::datetime::time_of_day>: break;
                default: return false;
            }
            break;
        }
        case k::time_point: {
            switch(a.type_index()) {
                case any::index<takatori::datetime::time_point>: break;
                default: return false;
            }
            break;
        }
        default:   //TODO add more conversions
            return false;
    }
    return true;
}

}
