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
#include "value_to_any.h"

#include <jogasaki/data/binary_string_value.h>

namespace jogasaki::utils {

/**
 * @brief accessor of the content value in given type
 * @warning for bool, the reference to std::int8_t is returned as proxy.
 */
template<typename T>
[[nodiscard]]
std::conditional_t<std::is_same_v<T, bool>, std::int8_t, T>
const& ref(value const& v) {
    using A = std::conditional_t<std::is_same_v<T, bool>, std::int8_t, T>;
    if(auto* p = std::get_if<A>(&v.body()); p != nullptr) {
        return *p;
    }
    fail_with_exception();
}

// convert the value type contained in any if necessary
bool value_to_any(value const& v, any& a) {
    switch(v.type_index()) {
        case value::index<std::monostate>: a = any{}; return true;
        case value::index<std::int8_t>: a = any{std::in_place_type<std::int8_t>, ref<std::int8_t>(v)}; return true;
        case value::index<std::int16_t>: a = any{std::in_place_type<std::int16_t>, ref<std::int16_t>(v)}; return true;
        case value::index<std::int32_t>: a = any{std::in_place_type<std::int32_t>, ref<std::int32_t>(v)}; return true;
        case value::index<std::int64_t>: a = any{std::in_place_type<std::int64_t>, ref<std::int64_t>(v)}; return true;
        case value::index<float>: a = any{std::in_place_type<float>, ref<float>(v)}; return true;
        case value::index<double>: a = any{std::in_place_type<double>, ref<double>(v)}; return true;
        case value::index<std::string>: a = any{std::in_place_type<accessor::text>, accessor::text{ref<std::string>(v)}}; return true;
        case value::index<data::binary_string_value>: a = any{std::in_place_type<accessor::binary>, accessor::binary{ref<data::binary_string_value>(v).str()}}; return true;
        case value::index<runtime_t<meta::field_type_kind::decimal>>: a = any{std::in_place_type<runtime_t<meta::field_type_kind::decimal>>, ref<runtime_t<meta::field_type_kind::decimal>>(v)}; return true;
        case value::index<runtime_t<meta::field_type_kind::date>>: a = any{std::in_place_type<runtime_t<meta::field_type_kind::date>>, ref<runtime_t<meta::field_type_kind::date>>(v)}; return true;
        case value::index<runtime_t<meta::field_type_kind::time_of_day>>: a = any{std::in_place_type<runtime_t<meta::field_type_kind::time_of_day>>, ref<runtime_t<meta::field_type_kind::time_of_day>>(v)}; return true;
        case value::index<runtime_t<meta::field_type_kind::time_point>>: a = any{std::in_place_type<runtime_t<meta::field_type_kind::time_point>>, ref<runtime_t<meta::field_type_kind::time_point>>(v)}; return true;
        case value::index<std::size_t>: a = any{std::in_place_type<std::size_t>, ref<std::size_t>(v)}; return true;
        default: break;
    }
    return false;
}

}
