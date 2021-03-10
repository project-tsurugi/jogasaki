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

#include <takatori/type/int.h>
#include <takatori/type/float.h>
#include <takatori/type/character.h>
#include <takatori/type/unknown.h>
#include <takatori/value/int.h>
#include <takatori/value/float.h>
#include <takatori/value/character.h>
#include <takatori/value/unknown.h>
#include <takatori/value/unknown_kind.h>

#include <jogasaki/accessor/text.h>

namespace jogasaki::plan {

using kind = meta::field_type_kind;

void parameter_set::set_int4(std::string_view name, runtime_t<kind::int4> value) {
    map_.add(std::string(name),
        {
            takatori::type::int4(),
            takatori::value::int4(value),
        }
    );
}

void parameter_set::set_int8(std::string_view name, runtime_t<kind::int8> value) {
    map_.add(std::string(name),
        {
            takatori::type::int8(),
            takatori::value::int8(value),
        }
    );
}

void parameter_set::set_float4(std::string_view name, runtime_t<kind::float4> value) {
    map_.add(std::string(name),
        {
            takatori::type::float4(),
            takatori::value::float4(value),
        }
    );
}

void parameter_set::set_float8(std::string_view name, runtime_t<kind::float8> value) {
    map_.add(std::string(name),
        {
            takatori::type::float8(),
            takatori::value::float8(value),
        }
    );
}

void parameter_set::set_character(std::string_view name, runtime_t<kind::character> value) {
    map_.add(std::string(name),
        {
            takatori::type::character(takatori::type::varying),
            takatori::value::character(static_cast<std::string_view>(value)),
        }
    );
}

void parameter_set::set_null(std::string_view name) {
    map_.add(std::string(name),
        {
            takatori::type::unknown(),
            takatori::value::unknown(takatori::value::unknown_kind::null),
        }
    );
}

mizugaki::placeholder_map const& parameter_set::map() const noexcept {
    return map_;
}

}
