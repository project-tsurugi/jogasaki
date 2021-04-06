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
#include "parameter_set.h"

namespace jogasaki::plan {

using kind = meta::field_type_kind;
using takatori::util::enum_tag;

void parameter_set::set_int4(std::string_view name, runtime_t<kind::int4> value) {
    add(std::string(name),
        {
            meta::field_type{enum_tag<kind::int4>},
            any{std::in_place_type<runtime_t<kind::int4>>, value}
        }
    );
}

void parameter_set::set_int8(std::string_view name, runtime_t<kind::int8> value) {
    add(std::string(name),
        {
            meta::field_type{enum_tag<kind::int8>},
            any{std::in_place_type<runtime_t<kind::int8>>, value}
        }
    );
}

void parameter_set::set_float4(std::string_view name, runtime_t<kind::float4> value) {
    add(std::string(name),
        {
            meta::field_type{enum_tag<kind::float4>},
            any{std::in_place_type<runtime_t<kind::float4>>, value}
        }
    );
}

void parameter_set::set_float8(std::string_view name, runtime_t<kind::float8> value) {
    add(std::string(name),
        {
            meta::field_type{enum_tag<kind::float8>},
            any{std::in_place_type<runtime_t<kind::float8>>, value}
        }
    );
}

void parameter_set::set_character(std::string_view name, runtime_t<kind::character> value) {
    add(std::string(name),
        {
            meta::field_type{enum_tag<kind::character>},
            any{std::in_place_type<runtime_t<kind::character>>, value}
        }
    );
}

void parameter_set::set_null(std::string_view name) {
    add(std::string(name),
        {
            meta::field_type{enum_tag<kind::undefined>},
            any{}
        }
    );
}

optional_ptr<parameter_set::entry_type const> parameter_set::find(std::string_view name) const {
    if (map_.count(std::string(name)) != 0) {
        return map_.at(std::string(name));
    }
    return {};
}

std::size_t parameter_set::size() const noexcept {
    return map_.size();
}

void parameter_set::add(std::string name, parameter_set::entry_type entry) {
    map_.insert_or_assign(std::move(name), std::move(entry));
}

parameter_set::iterator parameter_set::begin() const noexcept {
    return map_.begin();
}

parameter_set::iterator parameter_set::end() const noexcept {
    return map_.end();
}

}
