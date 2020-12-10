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
#include "builtin_functions.h"

#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/sequence_view.h>
#include <takatori/util/fail.h>
#include <takatori/util/enum_tag.h>

#include <jogasaki/constants.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/utils/copy_field_data.h>

namespace jogasaki::executor::builtin {

using takatori::util::maybe_shared_ptr;
using takatori::util::sequence_view;
using takatori::util::fail;
using takatori::util::enum_tag;

using kind = meta::field_type_kind;
template <kind Kind>
using rtype = typename meta::field_type_traits<Kind>::runtime_type;

void sum(
    accessor::record_ref target,
    field_locator const& target_loc,
    bool initial,
    accessor::record_ref source,
    sequence_view<field_locator const> args
) {
    BOOST_ASSERT(args.size() == 1);  //NOLINT
    auto& arg_type = args[0].type();
    auto arg_offset = args[0].value_offset();
    auto src_nullity_offset = args[0].value_offset();
    auto target_offset = target_loc.value_offset();
    auto target_nullity_offset = target_loc.nullity_offset();
    if (initial) {
        utils::copy_nullable_field(
            arg_type,
            target,
            target_offset,
            target_nullity_offset,
            source,
            arg_offset,
            src_nullity_offset
        );
        return;
    }
    auto is_null = source.is_null(src_nullity_offset);
    target.set_null(target_nullity_offset, is_null);
    if (is_null) return;
    switch(arg_type.kind()) {
        case kind::int4: target.set_value<rtype<kind::int4>>(target_offset, target.get_value<rtype<kind::int4>>(target_offset) + source.get_value<rtype<kind::int4>>(arg_offset)); break;
        case kind::int8: target.set_value<rtype<kind::int8>>(target_offset, target.get_value<rtype<kind::int8>>(target_offset) + source.get_value<rtype<kind::int8>>(arg_offset)); break;
        case kind::float4: target.set_value<rtype<kind::float4>>(target_offset, target.get_value<rtype<kind::float4>>(target_offset) + source.get_value<rtype<kind::float4>>(arg_offset)); break;
        case kind::float8: target.set_value<rtype<kind::float8>>(target_offset, target.get_value<rtype<kind::float8>>(target_offset) + source.get_value<rtype<kind::float8>>(arg_offset)); break;
        default: fail();
    }
}

void count(
    accessor::record_ref target,
    field_locator const& target_loc,
    bool initial,
    accessor::record_ref source,
    sequence_view<field_locator const> args
) {
    BOOST_ASSERT(args.size() == 1);  //NOLINT
    (void)args;
    (void)source;
    auto target_offset = target_loc.value_offset();
    auto target_nullity_offset = target_loc.nullity_offset();
    target.set_null(target_nullity_offset, false);
    if (initial) {
        target.set_value<rtype<kind::int8>>(target_offset, 1);
        return;
    }
    target.set_value<rtype<kind::int8>>(target_offset, target.get_value<rtype<kind::int8>>(target_offset) + 1);
}

}
