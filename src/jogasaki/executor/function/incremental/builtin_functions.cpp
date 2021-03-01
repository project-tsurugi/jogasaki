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

#include <takatori/util/sequence_view.h>
#include <takatori/util/fail.h>
#include <takatori/type/int.h>
#include <takatori/type/float.h>
#include <takatori/type/character.h>
#include <yugawara/aggregate/configurable_provider.h>

#include <jogasaki/executor/global.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/utils/copy_field_data.h>
#include <jogasaki/executor/function/incremental/aggregate_function_info.h>
#include <jogasaki/executor/function/incremental/aggregate_function_repository.h>

namespace jogasaki::executor::function::incremental {

using takatori::util::sequence_view;
using takatori::util::fail;

using kind = meta::field_type_kind;
template <kind Kind>
using rtype = typename meta::field_type_traits<Kind>::runtime_type;

void add_builtin_aggregate_functions(
    ::yugawara::aggregate::configurable_provider& functions,
    executor::function::incremental::aggregate_function_repository& repo
) {
    namespace t = takatori::type;
    using namespace ::yugawara;
    std::size_t id = aggregate::declaration::minimum_builtin_function_id;

    /////////
    // sum
    /////////
    auto sum = std::make_shared<aggregate_function_info_impl<aggregate_function_kind::sum>>();
    repo.add(id, sum);
    functions.add({
        id++,
        "sum",
        t::int4(),
        {
            t::int4(),
        },
        true,
    });
    repo.add(id, sum);
    functions.add({
        id++,
        "sum",
        t::int8(),
        {
            t::int8(),
        },
        true,
    });
    repo.add(id, sum);
    functions.add({
        id++,
        "sum",
        t::float4(),
        {
            t::float4(),
        },
        true,
    });
    repo.add(id, sum);
    functions.add({
        id++,
        "sum",
        t::float8(),
        {
            t::float8(),
        },
        true,
    });

    /////////
    // count
    /////////
    auto count = std::make_shared<aggregate_function_info_impl<aggregate_function_kind::count>>();
    repo.add(id, count);
    functions.add({
        id++,
        "count",
        t::int8(),
        {
            t::int4(),
        },
        true,
    });
    repo.add(id, count);
    functions.add({
        id++,
        "count",
        t::int8(),
        {
            t::int8(),
        },
        true,
    });
    repo.add(id, count);
    functions.add({
        id++,
        "count",
        t::int8(),
        {
            t::float4(),
        },
    });
    repo.add(id, count);
    functions.add({
        id++,
        "count",
        t::int8(),
        {
            t::float8(),
        },
        true,
    });

    /////////
    // avg
    /////////
    auto avg = std::make_shared<aggregate_function_info_impl<aggregate_function_kind::avg>>();
    repo.add(id, avg);
    functions.add({
        id++,
        "avg",
        t::int4(),
        {
            t::int4(),
        },
        true,
    });
    repo.add(id, avg);
    functions.add({
        id++,
        "avg",
        t::int8(),
        {
            t::int8(),
        },
        true,
    });
    repo.add(id, avg);
    functions.add({
        id++,
        "avg",
        t::float4(),
        {
            t::float4(),
        },
        true,
    });
    repo.add(id, avg);
    functions.add({
        id++,
        "avg",
        t::float8(),
        {
            t::float8(),
        },
        true,
    });
}

namespace builtin {

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
    BOOST_ASSERT(target_loc.type().kind() == arg_type.kind());  //NOLINT
    auto src_nullity_offset = args[0].nullity_offset();
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

void count_pre(
    accessor::record_ref target,
    field_locator const& target_loc,
    bool initial,
    accessor::record_ref source,
    sequence_view<field_locator const> args
) {
    BOOST_ASSERT(args.size() == 1);  //NOLINT
    BOOST_ASSERT(target_loc.type().kind() == kind::int8);  //NOLINT
    auto target_offset = target_loc.value_offset();
    auto target_nullity_offset = target_loc.nullity_offset();
    target.set_null(target_nullity_offset, false);
    std::int64_t cnt = source.is_null(args[0].nullity_offset()) ? 0 : 1;
    if (initial) {
        target.set_value<rtype<kind::int8>>(target_offset, cnt);
        return;
    }
    target.set_value<rtype<kind::int8>>(target_offset, target.get_value<rtype<kind::int8>>(target_offset) + cnt);
}

void count_mid(
    accessor::record_ref target,
    field_locator const& target_loc,
    bool initial,
    accessor::record_ref source,
    sequence_view<field_locator const> args
) {
    BOOST_ASSERT(args.size() == 1);  //NOLINT
    BOOST_ASSERT(args[0].type().kind() == kind::int8);  //NOLINT
    BOOST_ASSERT(target_loc.type().kind() == kind::int8);  //NOLINT
    (void)args;
    (void)source;
    auto arg_offset = args[0].value_offset();
    auto target_offset = target_loc.value_offset();
    auto target_nullity_offset = target_loc.nullity_offset();
    target.set_null(target_nullity_offset, false);
    if (initial) {
        target.set_value<rtype<kind::int8>>(target_offset, source.get_value<rtype<kind::int8>>(arg_offset));
        return;
    }
    target.set_value<rtype<kind::int8>>(target_offset, target.get_value<rtype<kind::int8>>(target_offset) + source.get_value<rtype<kind::int8>>(arg_offset));
}

void avg_post(
    accessor::record_ref target,
    field_locator const& target_loc,
    bool initial,
    accessor::record_ref source,
    sequence_view<field_locator const> args
) {
    BOOST_ASSERT(args.size() == 2);  //NOLINT
    (void)initial;
    auto& sum_type = args[0].type();
    auto sum_offset = args[0].value_offset();
    auto sum_nullity_offset = args[0].nullity_offset();
    auto& count_type = args[1].type();
    (void)count_type;
    BOOST_ASSERT(count_type.kind() == kind::int8);  //NOLINT
    BOOST_ASSERT(sum_type.kind() == target_loc.type().kind());  //NOLINT
    auto count_offset = args[1].value_offset();
    auto count_nullity_offset = args[1].nullity_offset();
    (void)count_nullity_offset;
    auto target_offset = target_loc.value_offset();
    auto target_nullity_offset = target_loc.nullity_offset();
    auto is_null = source.is_null(sum_nullity_offset);
    target.set_null(target_nullity_offset, is_null);
    if (is_null) return;
    switch(sum_type.kind()) {
        case kind::int4: target.set_value<rtype<kind::int4>>(target_offset, source.get_value<rtype<kind::int4>>(sum_offset) / source.get_value<rtype<kind::int8>>(count_offset)); break;
        case kind::int8: target.set_value<rtype<kind::int8>>(target_offset, source.get_value<rtype<kind::int8>>(sum_offset) / source.get_value<rtype<kind::int8>>(count_offset)); break;
        case kind::float4: target.set_value<rtype<kind::float4>>(target_offset, source.get_value<rtype<kind::float4>>(sum_offset) / source.get_value<rtype<kind::int8>>(count_offset)); break;
        case kind::float8: target.set_value<rtype<kind::float8>>(target_offset, source.get_value<rtype<kind::float8>>(sum_offset) / source.get_value<rtype<kind::int8>>(count_offset)); break;
        default: fail();
    }
}

void identity_post(
    accessor::record_ref target,
    field_locator const& target_loc,
    bool initial,
    accessor::record_ref source,
    sequence_view<field_locator const> args
) {
    BOOST_ASSERT(args.size() == 1);  //NOLINT
    (void)initial;
    auto& type = args[0].type();
    auto offset = args[0].value_offset();
    auto nullity_offset = args[0].nullity_offset();
    auto target_offset = target_loc.value_offset();
    auto target_nullity_offset = target_loc.nullity_offset();
    auto is_null = source.is_null(nullity_offset);
    target.set_null(target_nullity_offset, is_null);
    if (is_null) return;
    switch(type.kind()) {
        case kind::int4: target.set_value<rtype<kind::int4>>(target_offset, source.get_value<rtype<kind::int4>>(offset)); break;
        case kind::int8: target.set_value<rtype<kind::int8>>(target_offset, source.get_value<rtype<kind::int8>>(offset)); break;
        case kind::float4: target.set_value<rtype<kind::float4>>(target_offset, source.get_value<rtype<kind::float4>>(offset)); break;
        case kind::float8: target.set_value<rtype<kind::float8>>(target_offset, source.get_value<rtype<kind::float8>>(offset)); break;
        default: fail();
    }
}
}

}
