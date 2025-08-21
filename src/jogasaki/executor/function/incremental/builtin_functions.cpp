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
#include "builtin_functions.h"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <decimal.hh>
#include <memory>
#include <boost/assert.hpp>

#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/time_point.h>
#include <takatori/type/blob.h>
#include <takatori/type/character.h>
#include <takatori/type/clob.h>
#include <takatori/type/date.h>
#include <takatori/type/decimal.h>
#include <takatori/type/octet.h>
#include <takatori/type/primitive.h>
#include <takatori/type/time_of_day.h>
#include <takatori/type/time_point.h>
#include <takatori/type/type_kind.h>
#include <takatori/type/varying.h>
#include <takatori/util/sequence_view.h>
#include <yugawara/aggregate/configurable_provider.h>
#include <yugawara/aggregate/declaration.h>
#include <yugawara/util/maybe_shared_lock.h>

#include <jogasaki/accessor/binary.h>
#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/accessor/text.h>
#include <jogasaki/executor/function/field_locator.h>
#include <jogasaki/executor/function/incremental/aggregate_function_info.h>
#include <jogasaki/executor/function/incremental/aggregate_function_kind.h>
#include <jogasaki/executor/function/incremental/aggregate_function_repository.h>
#include <jogasaki/executor/function/incremental/builtin_functions_id.h>
#include <jogasaki/executor/less.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_traits.h>
#include <jogasaki/utils/copy_field_data.h>
#include <jogasaki/utils/fail.h>

namespace jogasaki::executor::function::incremental {

using takatori::util::sequence_view;

using kind = meta::field_type_kind;

void add_builtin_aggregate_functions(
    ::yugawara::aggregate::configurable_provider& functions,
    executor::function::incremental::aggregate_function_repository& repo
) {
    namespace t = takatori::type;
    using namespace ::yugawara;

    /////////
    // sum
    /////////
    {
        auto sum = std::make_shared<aggregate_function_info_impl<aggregate_function_kind::sum>>();
        auto id = function_id::id_10000;
        repo.add(id, sum);
        functions.add({
            id,
            "sum",
            t::int4(),
            {
                t::int4(),
            },
            true,
        });
        id = function_id::id_10001;
        repo.add(id, sum);
        functions.add({
            id,
            "sum",
            t::int8(),
            {
                t::int8(),
            },
            true,
        });
        id = function_id::id_10002;
        repo.add(id, sum);
        functions.add({
            id,
            "sum",
            t::float4(),
            {
                t::float4(),
            },
            true,
        });
        id = function_id::id_10003;
        repo.add(id, sum);
        functions.add({
            id,
            "sum",
            t::float8(),
            {
                t::float8(),
            },
            true,
        });
        id = function_id::id_10004;
        repo.add(id, sum);
        functions.add({
            id,
            "sum",
            t::decimal(),
            {
                t::decimal(),
            },
            true,
        });
    }

    /////////
    // count
    /////////
    {
        auto count = std::make_shared<aggregate_function_info_impl<aggregate_function_kind::count>>();
        auto id = function_id::id_10005;
        repo.add(id, count);
        functions.add({
            id,
            "count",
            t::int8(),
            {
                t::boolean(),
            },
            true,
        });
        id = function_id::id_10006;
        repo.add(id, count);
        functions.add({
            id,
            "count",
            t::int8(),
            {
                t::int4(),
            },
            true,
        });
        id = function_id::id_10007;
        repo.add(id, count);
        functions.add({
            id,
            "count",
            t::int8(),
            {
                t::int8(),
            },
            true,
        });
        id = function_id::id_10008;
        repo.add(id, count);
        functions.add({
            id,
            "count",
            t::int8(),
            {
                t::float4(),
            },
            true,
        });
        id = function_id::id_10009;
        repo.add(id, count);
        functions.add({
            id,
            "count",
            t::int8(),
            {
                t::float8(),
            },
            true,
        });
        id = function_id::id_10010;
        repo.add(id, count);
        functions.add({
            id,
            "count",
            t::int8(),
            {
                t::decimal(),
            },
            true,
        });
        id = function_id::id_10011;
        repo.add(id, count);
        functions.add({
            id,
            "count",
            t::int8(),
            {
                t::character(t::varying),
            },
            true,
        });
        id = function_id::id_10012;
        repo.add(id, count);
        functions.add({
            id,
            "count",
            t::int8(),
            {
                t::octet(t::varying),
            },
            true,
        });
        id = function_id::id_10013;
        repo.add(id, count);
        functions.add({
            id,
            "count",
            t::int8(),
            {
                t::date(),
            },
            true,
        });
        id = function_id::id_10014;
        repo.add(id, count);
        functions.add({
            id,
            "count",
            t::int8(),
            {
                t::time_of_day(),
            },
            true,
        });
        id = function_id::id_10015;
        repo.add(id, count);
        functions.add({
            id,
            "count",
            t::int8(),
            {
                t::time_of_day(t::with_time_zone),
            },
            true,
        });
        id = function_id::id_10016;
        repo.add(id, count);
        functions.add({
            id,
            "count",
            t::int8(),
            {
                t::time_point(),
            },
            true,
        });
        id = function_id::id_10017;
        repo.add(id, count);
        functions.add({
            id,
            "count",
            t::int8(),
            {
                t::time_point(t::with_time_zone),
            },
            true,
        });
        id = function_id::id_10048;
        repo.add(id, count);
        functions.add({
            id,
            "count",
            t::int8(),
            {
                t::blob(),
            },
            true,
        });
        id = function_id::id_10049;
        repo.add(id, count);
        functions.add({
            id,
            "count",
            t::int8(),
            {
                t::clob(),
            },
            true,
        });
    }

    /////////
    // count(*)
    /////////
    {
        auto count_rows = std::make_shared<aggregate_function_info_impl<aggregate_function_kind::count_rows>>();
        auto id = function_id::id_10018;
        repo.add(id, count_rows);
        functions.add({
            id,
            "count",
            t::int8(),
            {},
            true,
        });
    }

    /////////
    // avg
    /////////
    {
        auto avg = std::make_shared<aggregate_function_info_impl<aggregate_function_kind::avg>>();
        auto id = function_id::id_10019;
        repo.add(id, avg);
        functions.add({
            id,
            "avg",
            t::int4(),
            {
                t::int4(),
            },
            true,
        });
        id = function_id::id_10020;
        repo.add(id, avg);
        functions.add({
            id,
            "avg",
            t::int8(),
            {
                t::int8(),
            },
            true,
        });
        id = function_id::id_10021;
        repo.add(id, avg);
        functions.add({
            id,
            "avg",
            t::float4(),
            {
                t::float4(),
            },
            true,
        });
        id = function_id::id_10022;
        repo.add(id, avg);
        functions.add({
            id,
            "avg",
            t::float8(),
            {
                t::float8(),
            },
            true,
        });
        id = function_id::id_10023;
        repo.add(id, avg);
        functions.add({
            id,
            "avg",
            t::decimal(),
            {
                t::decimal(),
            },
            true,
        });
    }
    /////////
    // max
    /////////
    {
        auto max = std::make_shared<aggregate_function_info_impl<aggregate_function_kind::max>>();
        auto id = function_id::id_10024;
        repo.add(id, max);
        functions.add({
            id,
            "max",
            t::int4(),
            {
                t::int4(),
            },
            true,
        });
        id = function_id::id_10025;
        repo.add(id, max);
        functions.add({
            id,
            "max",
            t::int8(),
            {
                t::int8(),
            },
            true,
        });
        id = function_id::id_10026;
        repo.add(id, max);
        functions.add({
            id,
            "max",
            t::float4(),
            {
                t::float4(),
            },
            true,
        });
        id = function_id::id_10027;
        repo.add(id, max);
        functions.add({
            id,
            "max",
            t::float8(),
            {
                t::float8(),
            },
            true,
        });
        id = function_id::id_10028;
        repo.add(id, max);
        functions.add({
            id,
            "max",
            t::character(t::varying),
            {
                t::character(t::varying),
            },
            true,
        });
        id = function_id::id_10029;
        repo.add(id, max);
        functions.add({
            id,
            "max",
            t::octet(t::varying),
            {
                t::octet(t::varying),
            },
            true,
        });
        id = function_id::id_10030;
        repo.add(id, max);
        functions.add({
            id,
            "max",
            t::decimal(),
            {
                t::decimal(),
            },
            true,
        });
        id = function_id::id_10031;
        repo.add(id, max);
        functions.add({
            id,
            "max",
            t::date(),
            {
                t::date(),
            },
            true,
        });
        id = function_id::id_10032;
        repo.add(id, max);
        functions.add({
            id,
            "max",
            t::time_of_day(),
            {
                t::time_of_day(),
            },
            true,
        });
        id = function_id::id_10033;
        repo.add(id, max);
        functions.add({
            id,
            "max",
            t::time_of_day(t::with_time_zone),
            {
                t::time_of_day(t::with_time_zone),
            },
            true,
        });
        id = function_id::id_10034;
        repo.add(id, max);
        functions.add({
            id,
            "max",
            t::time_point(),
            {
                t::time_point(),
            },
            true,
        });
        id = function_id::id_10035;
        repo.add(id, max);
        functions.add({
            id,
            "max",
            t::time_point(t::with_time_zone),
            {
                t::time_point(t::with_time_zone),
            },
            true,
        });
    }
    /////////
    // min
    /////////
    {
        auto min = std::make_shared<aggregate_function_info_impl<aggregate_function_kind::min>>();
        auto id = function_id::id_10036;
        repo.add(id, min);
        functions.add({
            id,
            "min",
            t::int4(),
            {
                t::int4(),
            },
            true,
        });
        id = function_id::id_10037;
        repo.add(id, min);
        functions.add({
            id,
            "min",
            t::int8(),
            {
                t::int8(),
            },
            true,
        });
        id = function_id::id_10038;
        repo.add(id, min);
        functions.add({
            id,
            "min",
            t::float4(),
            {
                t::float4(),
            },
            true,
        });
        id = function_id::id_10039;
        repo.add(id, min);
        functions.add({
            id,
            "min",
            t::float8(),
            {
                t::float8(),
            },
            true,
        });
        id = function_id::id_10040;
        repo.add(id, min);
        functions.add({
            id,
            "min",
            t::character(t::varying),
            {
                t::character(t::varying),
            },
            true,
        });
        id = function_id::id_10041;
        repo.add(id, min);
        functions.add({
            id,
            "min",
            t::octet(t::varying),
            {
                t::octet(t::varying),
            },
            true,
        });
        id = function_id::id_10042;
        repo.add(id, min);
        functions.add({
            id,
            "min",
            t::decimal(),
            {
                t::decimal(),
            },
            true,
        });
        id = function_id::id_10043;
        repo.add(id, min);
        functions.add({
            id,
            "min",
            t::date(),
            {
                t::date(),
            },
            true,
        });
        id = function_id::id_10044;
        repo.add(id, min);
        functions.add({
            id,
            "min",
            t::time_of_day(),
            {
                t::time_of_day(),
            },
            true,
        });
        id = function_id::id_10045;
        repo.add(id, min);
        functions.add({
            id,
            "min",
            t::time_of_day(t::with_time_zone),
            {
                t::time_of_day(t::with_time_zone),
            },
            true,
        });
        id = function_id::id_10046;
        repo.add(id, min);
        functions.add({
            id,
            "min",
            t::time_point(),
            {
                t::time_point(),
            },
            true,
        });
        id = function_id::id_10047;
        repo.add(id, min);
        functions.add({
            id,
            "min",
            t::time_point(t::with_time_zone),
            {
                t::time_point(t::with_time_zone),
            },
            true,
        });
    }
}

namespace builtin {

template <class T>
static T plus(T a, T b) {
    return a + b;
}

template <>
runtime_t<kind::decimal> plus(runtime_t<kind::decimal> a, runtime_t<kind::decimal> b) {
    // TODO use context
    auto aa = static_cast<decimal::Decimal>(a);
    auto bb = static_cast<decimal::Decimal>(b);
    return runtime_t<kind::decimal>{(aa + bb).as_uint128_triple()};
}

void sum(
    accessor::record_ref target,
    field_locator const& target_loc,
    bool initial,
    accessor::record_ref source,
    sequence_view<field_locator const> args,
    memory::paged_memory_resource* // `sum` does not create new varlen data
) {
    BOOST_ASSERT(args.size() == 1);  //NOLINT
    auto& arg_type = args[0].type();
    auto arg_offset = args[0].value_offset();
    BOOST_ASSERT(target_loc.type().kind() == arg_type.kind());  //NOLINT
    auto src_nullity_offset = args[0].nullity_offset();
    auto target_offset = target_loc.value_offset();
    auto target_nullity_offset = target_loc.nullity_offset();
    auto src_is_null = source.is_null(src_nullity_offset);
    auto tgt_is_null = target.is_null(target_nullity_offset);
    if (initial || tgt_is_null) {
        // target value is empty, do not use
        target.set_null(target_nullity_offset, src_is_null);
        if (src_is_null) return;
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
    if (src_is_null) return;
    switch(arg_type.kind()) {
        case kind::int4: target.set_value<runtime_t<kind::int4>>(target_offset, plus(target.get_value<runtime_t<kind::int4>>(target_offset), source.get_value<runtime_t<kind::int4>>(arg_offset))); break;
        case kind::int8: target.set_value<runtime_t<kind::int8>>(target_offset, plus(target.get_value<runtime_t<kind::int8>>(target_offset), source.get_value<runtime_t<kind::int8>>(arg_offset))); break;
        case kind::float4: target.set_value<runtime_t<kind::float4>>(target_offset, plus(target.get_value<runtime_t<kind::float4>>(target_offset), source.get_value<runtime_t<kind::float4>>(arg_offset))); break;
        case kind::float8: target.set_value<runtime_t<kind::float8>>(target_offset, plus(target.get_value<runtime_t<kind::float8>>(target_offset), source.get_value<runtime_t<kind::float8>>(arg_offset))); break;
        case kind::decimal: target.set_value<runtime_t<kind::decimal>>(target_offset, plus(target.get_value<runtime_t<kind::decimal>>(target_offset), source.get_value<runtime_t<kind::decimal>>(arg_offset))); break;
        default: fail_with_exception();
    }
}

void count_pre(
    accessor::record_ref target,
    field_locator const& target_loc,
    bool initial,
    accessor::record_ref source,
    sequence_view<field_locator const> args,
    memory::paged_memory_resource* // `count` does not create new varlen data
) {
    BOOST_ASSERT(args.size() == 1);  //NOLINT
    BOOST_ASSERT(target_loc.type().kind() == kind::int8);  //NOLINT
    auto target_offset = target_loc.value_offset();
    auto target_nullity_offset = target_loc.nullity_offset();
    target.set_null(target_nullity_offset, false); // `count` never returns null
    std::int64_t cnt = source.is_null(args[0].nullity_offset()) ? 0 : 1;
    if (initial) {
        target.set_value<runtime_t<kind::int8>>(target_offset, cnt);
        return;
    }
    target.set_value<runtime_t<kind::int8>>(target_offset, target.get_value<runtime_t<kind::int8>>(target_offset) + cnt);
}

void count_mid(
    accessor::record_ref target,
    field_locator const& target_loc,
    bool initial,
    accessor::record_ref source,
    sequence_view<field_locator const> args,
    memory::paged_memory_resource* // `count` does not create new varlen data
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
        // non-null value must have been assigned by count_pre
        target.set_value<runtime_t<kind::int8>>(target_offset, source.get_value<runtime_t<kind::int8>>(arg_offset));
        return;
    }
    target.set_value<runtime_t<kind::int8>>(target_offset, target.get_value<runtime_t<kind::int8>>(target_offset) + source.get_value<runtime_t<kind::int8>>(arg_offset));
}

void count_rows_pre(
    accessor::record_ref target,
    field_locator const& target_loc,
    bool initial,
    accessor::record_ref source,
    sequence_view<field_locator const> args,
    memory::paged_memory_resource* // `count` does not create new varlen data
) {
    BOOST_ASSERT(args.size() == 0);  //NOLINT
    BOOST_ASSERT(target_loc.type().kind() == kind::int8);  //NOLINT
    (void)args;
    (void)source;
    auto target_offset = target_loc.value_offset();
    auto target_nullity_offset = target_loc.nullity_offset();
    target.set_null(target_nullity_offset, false); // `count(*)` never returns null
    if (initial) {
        target.set_value<runtime_t<kind::int8>>(target_offset, 1);
        return;
    }
    target.set_value<runtime_t<kind::int8>>(target_offset, target.get_value<runtime_t<kind::int8>>(target_offset) + 1);
}

template <class T>
static T div_by_count(T a, runtime_t<kind::int8> b) {
    return a / b;
}

template <>
runtime_t<kind::decimal> div_by_count(runtime_t<kind::decimal> a, runtime_t<kind::int8> b) {
    // TODO add context
    auto aa = static_cast<decimal::Decimal>(a);
    return runtime_t<kind::decimal>{(aa / b).as_uint128_triple()};
}

void avg_post(
    accessor::record_ref target,
    field_locator const& target_loc,
    bool initial,
    accessor::record_ref source,
    sequence_view<field_locator const> args,
    memory::paged_memory_resource* // `avg` does not create new varlen data
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
        case kind::int4: target.set_value<runtime_t<kind::int4>>(target_offset, div_by_count(source.get_value<runtime_t<kind::int4>>(sum_offset), source.get_value<runtime_t<kind::int8>>(count_offset))); break;
        case kind::int8: target.set_value<runtime_t<kind::int8>>(target_offset, div_by_count(source.get_value<runtime_t<kind::int8>>(sum_offset), source.get_value<runtime_t<kind::int8>>(count_offset))); break;
        case kind::float4: target.set_value<runtime_t<kind::float4>>(target_offset, div_by_count(source.get_value<runtime_t<kind::float4>>(sum_offset), source.get_value<runtime_t<kind::int8>>(count_offset))); break;
        case kind::float8: target.set_value<runtime_t<kind::float8>>(target_offset, div_by_count(source.get_value<runtime_t<kind::float8>>(sum_offset), source.get_value<runtime_t<kind::int8>>(count_offset))); break;
        case kind::decimal: target.set_value<runtime_t<kind::decimal>>(target_offset, div_by_count(source.get_value<runtime_t<kind::decimal>>(sum_offset), source.get_value<runtime_t<kind::int8>>(count_offset))); break;
        default: fail_with_exception();
    }
}

template <class T>
static T max_or_min(bool max, T a, T b) {
    if (less(a,b)) {
        return max ? b : a;
    }
    return max ? a : b;
}

void max(
    accessor::record_ref target,
    field_locator const& target_loc,
    bool initial,
    accessor::record_ref source,
    sequence_view<field_locator const> args,
    memory::paged_memory_resource* varlen_resource // `max` needs to remember the maximum value
) {
    BOOST_ASSERT(args.size() == 1);  //NOLINT
    auto& arg_type = args[0].type();
    auto arg_offset = args[0].value_offset();
    BOOST_ASSERT(target_loc.type().kind() == arg_type.kind());  //NOLINT
    auto src_nullity_offset = args[0].nullity_offset();
    auto target_offset = target_loc.value_offset();
    auto target_nullity_offset = target_loc.nullity_offset();
    auto src_is_null = source.is_null(src_nullity_offset);
    auto tgt_is_null = target.is_null(target_nullity_offset);
    if (initial || tgt_is_null) {
        // target value is empty, do not use
        target.set_null(target_nullity_offset, src_is_null);
        if (src_is_null) return;
        utils::copy_nullable_field(
            arg_type,
            target,
            target_offset,
            target_nullity_offset,
            source,
            arg_offset,
            src_nullity_offset,
            varlen_resource // copy upstream process varlen data to exchange's varlen_resource
        );
        return;
    }
    if (src_is_null) return;
    switch(arg_type.kind()) {
        case kind::int4: target.set_value<runtime_t<kind::int4>>(target_offset, max_or_min(true, target.get_value<runtime_t<kind::int4>>(target_offset), source.get_value<runtime_t<kind::int4>>(arg_offset))); break;
        case kind::int8: target.set_value<runtime_t<kind::int8>>(target_offset, max_or_min(true, target.get_value<runtime_t<kind::int8>>(target_offset), source.get_value<runtime_t<kind::int8>>(arg_offset))); break;
        case kind::float4: target.set_value<runtime_t<kind::float4>>(target_offset, max_or_min(true, target.get_value<runtime_t<kind::float4>>(target_offset), source.get_value<runtime_t<kind::float4>>(arg_offset))); break;
        case kind::float8: target.set_value<runtime_t<kind::float8>>(target_offset, max_or_min(true, target.get_value<runtime_t<kind::float8>>(target_offset), source.get_value<runtime_t<kind::float8>>(arg_offset))); break;

        // character and octet uses varlen_resource to copy varlen data owned by upstream process
        case kind::character: target.set_value<runtime_t<kind::character>>(target_offset, accessor::text{varlen_resource, max_or_min(true, target.get_value<runtime_t<kind::character>>(target_offset), source.get_value<runtime_t<kind::character>>(arg_offset))}); break;
        case kind::octet: target.set_value<runtime_t<kind::octet>>(target_offset, accessor::binary{varlen_resource, max_or_min(true, target.get_value<runtime_t<kind::octet>>(target_offset), source.get_value<runtime_t<kind::octet>>(arg_offset))}); break;

        case kind::decimal: target.set_value<runtime_t<kind::decimal>>(target_offset, max_or_min(true, target.get_value<runtime_t<kind::decimal>>(target_offset), source.get_value<runtime_t<kind::decimal>>(arg_offset))); break;
        case kind::date: target.set_value<runtime_t<kind::date>>(target_offset, max_or_min(true, target.get_value<runtime_t<kind::date>>(target_offset), source.get_value<runtime_t<kind::date>>(arg_offset))); break;
        case kind::time_of_day: target.set_value<runtime_t<kind::time_of_day>>(target_offset, max_or_min(true, target.get_value<runtime_t<kind::time_of_day>>(target_offset), source.get_value<runtime_t<kind::time_of_day>>(arg_offset))); break;
        case kind::time_point: target.set_value<runtime_t<kind::time_point>>(target_offset, max_or_min(true, target.get_value<runtime_t<kind::time_point>>(target_offset), source.get_value<runtime_t<kind::time_point>>(arg_offset))); break;
        default: fail_with_exception();
    }
}

void min(
    accessor::record_ref target,
    field_locator const& target_loc,
    bool initial,
    accessor::record_ref source,
    sequence_view<field_locator const> args,
    memory::paged_memory_resource* varlen_resource // `min` needs to remember the minimum value
) {
    BOOST_ASSERT(args.size() == 1);  //NOLINT
    auto& arg_type = args[0].type();
    auto arg_offset = args[0].value_offset();
    BOOST_ASSERT(target_loc.type().kind() == arg_type.kind());  //NOLINT
    auto src_nullity_offset = args[0].nullity_offset();
    auto target_offset = target_loc.value_offset();
    auto target_nullity_offset = target_loc.nullity_offset();
    auto src_is_null = source.is_null(src_nullity_offset);
    auto tgt_is_null = target.is_null(target_nullity_offset);
    if (initial || tgt_is_null) {
        // target value is empty, do not use
        target.set_null(target_nullity_offset, src_is_null);
        if (src_is_null) return;
        utils::copy_nullable_field(
            arg_type,
            target,
            target_offset,
            target_nullity_offset,
            source,
            arg_offset,
            src_nullity_offset,
            varlen_resource  // copy upstream process varlen data to exchange's varlen_resource
        );
        return;
    }
    if (src_is_null) return;
    switch(arg_type.kind()) {
        case kind::int4: target.set_value<runtime_t<kind::int4>>(target_offset, max_or_min(false, target.get_value<runtime_t<kind::int4>>(target_offset), source.get_value<runtime_t<kind::int4>>(arg_offset))); break;
        case kind::int8: target.set_value<runtime_t<kind::int8>>(target_offset, max_or_min(false, target.get_value<runtime_t<kind::int8>>(target_offset), source.get_value<runtime_t<kind::int8>>(arg_offset))); break;
        case kind::float4: target.set_value<runtime_t<kind::float4>>(target_offset, max_or_min(false, target.get_value<runtime_t<kind::float4>>(target_offset), source.get_value<runtime_t<kind::float4>>(arg_offset))); break;
        case kind::float8: target.set_value<runtime_t<kind::float8>>(target_offset, max_or_min(false, target.get_value<runtime_t<kind::float8>>(target_offset), source.get_value<runtime_t<kind::float8>>(arg_offset))); break;

        // character and octet uses varlen_resource to copy varlen data owned by upstream process
        case kind::character: target.set_value<runtime_t<kind::character>>(target_offset, accessor::text{varlen_resource, max_or_min(false, target.get_value<runtime_t<kind::character>>(target_offset), source.get_value<runtime_t<kind::character>>(arg_offset))}); break;
        case kind::octet: target.set_value<runtime_t<kind::octet>>(target_offset, accessor::binary{varlen_resource, max_or_min(false, target.get_value<runtime_t<kind::octet>>(target_offset), source.get_value<runtime_t<kind::octet>>(arg_offset))}); break;

        case kind::decimal: target.set_value<runtime_t<kind::decimal>>(target_offset, max_or_min(false, target.get_value<runtime_t<kind::decimal>>(target_offset), source.get_value<runtime_t<kind::decimal>>(arg_offset))); break;
        case kind::date: target.set_value<runtime_t<kind::date>>(target_offset, max_or_min(false, target.get_value<runtime_t<kind::date>>(target_offset), source.get_value<runtime_t<kind::date>>(arg_offset))); break;
        case kind::time_of_day: target.set_value<runtime_t<kind::time_of_day>>(target_offset, max_or_min(false, target.get_value<runtime_t<kind::time_of_day>>(target_offset), source.get_value<runtime_t<kind::time_of_day>>(arg_offset))); break;
        case kind::time_point: target.set_value<runtime_t<kind::time_point>>(target_offset, max_or_min(false, target.get_value<runtime_t<kind::time_point>>(target_offset), source.get_value<runtime_t<kind::time_point>>(arg_offset))); break;
        default: fail_with_exception();
    }
}

void identity_post(
    accessor::record_ref target,
    field_locator const& target_loc,
    bool initial,
    accessor::record_ref source,
    sequence_view<field_locator const> args,
    memory::paged_memory_resource* // assuming record is already copied to the exchange's varlen_resource in pre/mid
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
        case kind::boolean: target.set_value<runtime_t<kind::boolean>>(target_offset, source.get_value<runtime_t<kind::boolean>>(offset)); break;
        case kind::int4: target.set_value<runtime_t<kind::int4>>(target_offset, source.get_value<runtime_t<kind::int4>>(offset)); break;
        case kind::int8: target.set_value<runtime_t<kind::int8>>(target_offset, source.get_value<runtime_t<kind::int8>>(offset)); break;
        case kind::float4: target.set_value<runtime_t<kind::float4>>(target_offset, source.get_value<runtime_t<kind::float4>>(offset)); break;
        case kind::float8: target.set_value<runtime_t<kind::float8>>(target_offset, source.get_value<runtime_t<kind::float8>>(offset)); break;
        case kind::decimal: target.set_value<runtime_t<kind::decimal>>(target_offset, source.get_value<runtime_t<kind::decimal>>(offset)); break;
        case kind::character: target.set_value<runtime_t<kind::character>>(target_offset, source.get_value<runtime_t<kind::character>>(offset)); break;
        case kind::octet: target.set_value<runtime_t<kind::octet>>(target_offset, source.get_value<runtime_t<kind::octet>>(offset)); break;
        case kind::date: target.set_value<runtime_t<kind::date>>(target_offset, source.get_value<runtime_t<kind::date>>(offset)); break;
        case kind::time_of_day: target.set_value<runtime_t<kind::time_of_day>>(target_offset, source.get_value<runtime_t<kind::time_of_day>>(offset)); break;
        case kind::time_point: target.set_value<runtime_t<kind::time_point>>(target_offset, source.get_value<runtime_t<kind::time_point>>(offset)); break;
        default: fail_with_exception();
    }
}

}  // namespace builtin

}  // namespace jogasaki::executor::function::incremental
