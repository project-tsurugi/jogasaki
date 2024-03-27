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
#include "value_generator.h"

#include <boost/assert.hpp>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/executor/function/field_locator.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_traits.h>
#include <jogasaki/utils/fail.h>

namespace jogasaki::executor::function {

using kind = meta::field_type_kind;

void null_generator(
    accessor::record_ref target,
    field_locator const& target_loc
) {
    BOOST_ASSERT(target_loc.nullable()); //NOLINT
    auto target_nullity_offset = target_loc.nullity_offset();
    target.set_null(target_nullity_offset, true);
}

void zero_generator(
    accessor::record_ref target,
    field_locator const& target_loc
) {
    auto target_offset = target_loc.value_offset();
    auto target_nullity_offset = target_loc.nullity_offset();
    target.set_null(target_nullity_offset, false);
    switch(target_loc.type().kind()) {
        case kind::int4: target.set_value<runtime_t<kind::int4>>(target_offset, 0); break;
        case kind::int8: target.set_value<runtime_t<kind::int8>>(target_offset, 0); break;
        case kind::float4: target.set_value<runtime_t<kind::float4>>(target_offset, 0); break;
        case kind::float8: target.set_value<runtime_t<kind::float8>>(target_offset, 0); break;
        case kind::decimal: target.set_value<runtime_t<kind::decimal>>(target_offset, {}); break;
        default: fail_with_exception();
    }
}

}
