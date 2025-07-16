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
#include "value_to_any.h"

#include <jogasaki/data/binary_string_value.h>

namespace jogasaki::utils {

// convert the value type contained in any if necessary
bool value_to_any(value const& v, any& a) {
    switch(v.type_index()) {
        case value::index<std::monostate>: a = any{}; return true;
        case value::index<std::int8_t>: a = any{std::in_place_type<std::int8_t>, v.ref<std::int8_t>()}; return true;
        case value::index<std::int16_t>: a = any{std::in_place_type<std::int16_t>, v.ref<std::int16_t>()}; return true;
        case value::index<std::int32_t>: a = any{std::in_place_type<std::int32_t>, v.ref<std::int32_t>()}; return true;
        case value::index<std::int64_t>: a = any{std::in_place_type<std::int64_t>, v.ref<std::int64_t>()}; return true;
        case value::index<float>: a = any{std::in_place_type<float>, v.ref<float>()}; return true;
        case value::index<double>: a = any{std::in_place_type<double>, v.ref<double>()}; return true;
        case value::index<std::string>: a = any{std::in_place_type<accessor::text>, accessor::text{v.ref<std::string>()}}; return true;
        case value::index<data::binary_string_value>: a = any{std::in_place_type<accessor::binary>, accessor::binary{v.ref<data::binary_string_value>().str()}}; return true;
        case value::index<runtime_t<meta::field_type_kind::decimal>>: a = any{std::in_place_type<runtime_t<meta::field_type_kind::decimal>>, v.ref<runtime_t<meta::field_type_kind::decimal>>()}; return true;
        case value::index<runtime_t<meta::field_type_kind::date>>: a = any{std::in_place_type<runtime_t<meta::field_type_kind::date>>, v.ref<runtime_t<meta::field_type_kind::date>>()}; return true;
        case value::index<runtime_t<meta::field_type_kind::time_of_day>>: a = any{std::in_place_type<runtime_t<meta::field_type_kind::time_of_day>>, v.ref<runtime_t<meta::field_type_kind::time_of_day>>()}; return true;
        case value::index<runtime_t<meta::field_type_kind::time_point>>: a = any{std::in_place_type<runtime_t<meta::field_type_kind::time_point>>, v.ref<runtime_t<meta::field_type_kind::time_point>>()}; return true;
        case value::index<std::size_t>: a = any{std::in_place_type<std::size_t>, v.ref<std::size_t>()}; return true;
        default: break;
    }
    return false;
}

}
