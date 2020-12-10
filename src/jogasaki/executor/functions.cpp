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

    functions.add({
        aggregate::declaration::minimum_system_function_id + 1,
        "sum",
        t::int8(),
        {
            t::int8(),
        },
        true,
    });
    functions.add({
        aggregate::declaration::minimum_system_function_id + 2,
        "sum",
        t::float8(),
        {
            t::float8(),
        },
        true,
    });
    functions.add({
        aggregate::declaration::minimum_system_function_id + 3,
        "count",
        t::int8(),
        {
            t::int8(),
        },
        true,
    });
    functions.add({
        aggregate::declaration::minimum_system_function_id + 4,
        "count",
        t::float8(),
        {
            t::int8(),
        },
        true,
    });
}

}
