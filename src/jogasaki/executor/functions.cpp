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
#include "functions.h"

#include <takatori/type/int.h>
#include <takatori/type/float.h>
#include <takatori/type/character.h>
#include <yugawara/aggregate/configurable_provider.h>

namespace jogasaki::executor {

using takatori::util::maybe_shared_ptr;
using takatori::util::sequence_view;
using takatori::util::fail;
using takatori::util::enum_tag;

void add_builtin_aggregate_functions(::yugawara::aggregate::configurable_provider& functions) {
    namespace t = takatori::type;
    using namespace ::yugawara;
    std::size_t id = aggregate::declaration::minimum_builtin_function_id;
    functions.add({
        id++,
        "sum",
        t::int4(),
        {
            t::int4(),
        },
        true,
    });
    functions.add({
        id++,
        "sum",
        t::int8(),
        {
            t::int8(),
        },
        true,
    });
    functions.add({
        id++,
        "sum",
        t::float4(),
        {
            t::float4(),
        },
        true,
    });
    functions.add({
        id++,
        "sum",
        t::float8(),
        {
            t::float8(),
        },
        true,
    });

    functions.add({
        id++,
        "count",
        t::int8(),
        {
            t::int4(),
        },
        true,
    });
    functions.add({
        id++,
        "count",
        t::int8(),
        {
            t::int8(),
        },
        true,
    });
    functions.add({
        id++,
        "count",
        t::int8(),
        {
            t::float4(),
        },
    });
    functions.add({
        id++,
        "count",
        t::int8(),
        {
            t::float8(),
        },
        true,
    });
}

field_locator::field_locator(const meta::field_type &type, bool nullable, std::size_t value_offset,
    std::size_t nullity_offset) :
    type_(std::addressof(type)),
    nullable_(nullable),
    value_offset_(value_offset),
    nullity_offset_(nullity_offset)
{}

meta::field_type const &field_locator::type() const noexcept {
    return *type_;
}

bool field_locator::nullable() const noexcept {
    return nullable_;
}

std::size_t field_locator::value_offset() const noexcept {
    return value_offset_;
}

std::size_t field_locator::nullity_offset() const noexcept {
    return nullity_offset_;
}
}
