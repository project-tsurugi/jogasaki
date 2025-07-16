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
#include "validate_any_type.h"

#include <memory>
#include <string>
#include <string_view>

#include <jogasaki/data/any.h>
#include <jogasaki/meta/field_type.h>

namespace jogasaki::utils {

bool validate_any_type(data::any a, meta::field_type const& f) {
    if(! a) {
        // empty or error
        return true;
    }
    using k = meta::field_type_kind;
    switch(f.kind()) {
        case k::boolean: return a.type_index() == data::any::index<runtime_t<k::boolean>>;
        case k::int4: return a.type_index() == data::any::index<runtime_t<k::int4>>;
        case k::int8: return a.type_index() == data::any::index<runtime_t<k::int8>>;
        case k::float4: return a.type_index() == data::any::index<runtime_t<k::float4>>;
        case k::float8: return a.type_index() == data::any::index<runtime_t<k::float8>>;
        case k::decimal: return a.type_index() == data::any::index<runtime_t<k::decimal>>;
        case k::character: return a.type_index() == data::any::index<runtime_t<k::character>>;
        case k::octet: return a.type_index() == data::any::index<runtime_t<k::octet>>;
        case k::date: return a.type_index() == data::any::index<runtime_t<k::date>>;
        case k::time_of_day: return a.type_index() == data::any::index<runtime_t<k::time_of_day>>;
        case k::time_point: return a.type_index() == data::any::index<runtime_t<k::time_point>>;
        default: return false;
    }
    return false;
}

}  // namespace jogasaki::utils
