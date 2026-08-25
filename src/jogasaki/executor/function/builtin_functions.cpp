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
#include "builtin_functions.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <variant>
#include <boost/container/pmr/polymorphic_allocator.hpp>
#include <tsl/hopscotch_hash.h>
#include <tsl/hopscotch_set.h>

#include <takatori/datetime/date.h>
#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/time_point.h>
#include <takatori/decimal/triple.h>
#include <takatori/type/character.h>
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
#include <jogasaki/executor/function/aggregate_function_info.h>
#include <jogasaki/executor/function/aggregate_function_kind.h>
#include <jogasaki/executor/function/aggregate_function_repository.h>
#include <jogasaki/executor/function/builtin_functions_id.h>
#include <jogasaki/executor/function/field_locator.h>
#include <jogasaki/executor/function/value_generator.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/memory/page_or_heap_memory_resource.h>
#include <jogasaki/memory/page_pool.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_traits.h>
#include <jogasaki/utils/assert.h>
#include <jogasaki/utils/fail.h>
#include <jogasaki/utils/round.h>

namespace jogasaki::executor::function {

using takatori::util::sequence_view;

using kind = meta::field_type_kind;

void add_builtin_aggregate_functions(
    ::yugawara::aggregate::configurable_provider& functions,
    executor::function::aggregate_function_repository& repo
) {
    namespace t = takatori::type;
    using namespace ::yugawara;

    /////////
    // count distinct
    /////////
    {
        auto count_distinct = std::make_shared<aggregate_function_info>(
            aggregate_function_kind::count_distinct,
            zero_generator,
            builtin::count_distinct
        );
        std::stringstream ss{};
        ss << "count";
        ss << ::yugawara::aggregate::declaration::name_suffix_distinct;
        auto name = ss.str();
        auto id = function_id::id_11000;
        repo.add(id, count_distinct);
        functions.add({
            id,
            name,
            t::int8(),
            {
                t::boolean(),
            },
            false,
        });
        id = function_id::id_11001;
        repo.add(id, count_distinct);
        functions.add({
            id,
            name,
            t::int8(),
            {
                t::int4(),
            },
            false,
        });
        id = function_id::id_11002;
        repo.add(id, count_distinct);
        functions.add({
            id,
            name,
            t::int8(),
            {
                t::int8(),
            },
            false,
        });
        id = function_id::id_11003;
        repo.add(id, count_distinct);
        functions.add({
            id,
            name,
            t::int8(),
            {
                t::float4(),
            },
        });
        id = function_id::id_11004;
        repo.add(id, count_distinct);
        functions.add({
            id,
            name,
            t::int8(),
            {
                t::float8(),
            },
            false,
        });
        id = function_id::id_11005;
        repo.add(id, count_distinct);
        functions.add({
            id,
            name,
            t::int8(),
            {
                t::character(t::varying),
            },
            false,
        });
        id = function_id::id_11006;
        repo.add(id, count_distinct);
        functions.add({
            id,
            name,
            t::int8(),
            {
                t::octet(t::varying),
            },
            false,
        });
        id = function_id::id_11007;
        repo.add(id, count_distinct);
        functions.add({
            id,
            name,
            t::int8(),
            {
                t::decimal(),
            },
            false,
        });
        id = function_id::id_11008;
        repo.add(id, count_distinct);
        functions.add({
            id,
            name,
            t::int8(),
            {
                t::date(),
            },
            false,
        });
        id = function_id::id_11009;
        repo.add(id, count_distinct);
        functions.add({
            id,
            name,
            t::int8(),
            {
                t::time_of_day(),
            },
            false,
        });
        id = function_id::id_11010;
        repo.add(id, count_distinct);
        functions.add({
            id,
            name,
            t::int8(),
            {
                t::time_of_day(takatori::type::with_time_zone),
            },
            false,
        });
        id = function_id::id_11011;
        repo.add(id, count_distinct);
        functions.add({
            id,
            name,
            t::int8(),
            {
                t::time_point(),
            },
            false,
        });
        id = function_id::id_11012;
        repo.add(id, count_distinct);
        functions.add({
            id,
            name,
            t::int8(),
            {
                t::time_point(takatori::type::with_time_zone),
            },
            false,
        });
    }
}

namespace builtin {

namespace details {

template<class T>
static std::int64_t count_distinct(data::value_store const& store) {
    using hash_table_allocator = boost::container::pmr::polymorphic_allocator<T>;
    using hash_set = tsl::hopscotch_set<T, std::hash<T>, std::equal_to<>, hash_table_allocator>;

    // The bucket array is a single contiguous allocation of hopscotch buckets.
    // Its element type depends on the neighborhood size. We borrow the default used in tsl::hopscotch_set.
    // We need it to compute how many buckets fit in a single pooled page.
    constexpr std::size_t neighborhood_size = 62;
    using bucket_type = tsl::detail_hopscotch_hash::hopscotch_bucket<T, neighborhood_size, false>;

    // The largest power-of-two bucket count whose array still fits in a single page. tsl rounds a
    // requested bucket count up to a power of two (power_of_two_growth_policy) and then allocates
    // bucket_count + neighborhood_size - 1 buckets (extra trailing buckets so the neighborhood of
    // the last bucket stays in bounds).
    // For safety, let's round down to a power of two after subtracting the neighborhood size so we don't exceed a page.
    constexpr std::size_t page_fitting_buckets = utils::round_down_to_power_of_two(
        memory::page_size / sizeof(bucket_type) - (neighborhood_size - 1));

    // verify the computation above really keeps the initial bucket array within one page.
    // This is in case for the internal change in tsl hopscotch_set implementation.
    static_assert(
        sizeof(bucket_type) * (page_fitting_buckets + neighborhood_size - 1) <= memory::page_size);

    // Serve the bucket array from a pooled page while it fits, and spill to the heap once it grows
    // past a page. This avoids the bad_alloc that occurred when a page-based resource had to serve a
    // single allocation larger than one page for large distinct cardinality.
    memory::page_or_heap_memory_resource resource{std::addressof(global::page_pool())};

    // Pre-size the table to the largest bucket count that still fits in a single pooled page so the
    // common large-cardinality case allocates once and rehashes zero times. Tables larger than that
    // grow onto the heap by doubling.
    // This intentionally optimizes for groups with many records: the initial bucket array is
    // allocated (and zero-initialized) at its full page-fitting size regardless of the actual input
    // size. TODO revisit the initial size
    hash_set values{page_fitting_buckets, hash_table_allocator{std::addressof(resource)}};

    auto b = store.begin<T>();
    auto e = store.end<T>();
    while(b != e) {
        if (! b.is_null()) {
            values.emplace(*b);
        }
        ++b;
    }
    return values.size();
}

} // namespace details

void count_distinct(
    accessor::record_ref target,
    field_locator const& target_loc,
    sequence_view<std::reference_wrapper<data::value_store> const> args
) {
    assert_with_exception(args.size() == 1, args.size());
    assert_with_exception(target_loc.type().kind() == kind::int8, target_loc.type().kind(), kind::int8);
    auto target_offset = target_loc.value_offset();
    auto target_nullity_offset = target_loc.nullity_offset();
    target.set_null(target_nullity_offset, false);
    auto& store = static_cast<data::value_store&>(args[0]);
    std::int64_t res{};
    switch(store.type().kind()) {
        case kind::boolean: res = details::count_distinct<runtime_t<kind::boolean>>(store); break;
        case kind::int4: res = details::count_distinct<runtime_t<kind::int4>>(store); break;
        case kind::int8: res = details::count_distinct<runtime_t<kind::int8>>(store); break;
        case kind::float4: res = details::count_distinct<runtime_t<kind::float4>>(store); break;
        case kind::float8: res = details::count_distinct<runtime_t<kind::float8>>(store); break;
        case kind::decimal: res = details::count_distinct<runtime_t<kind::decimal>>(store); break;
        case kind::character: res = details::count_distinct<runtime_t<kind::character>>(store); break;
        case kind::octet: res = details::count_distinct<runtime_t<kind::octet>>(store); break;
        case kind::date: res = details::count_distinct<runtime_t<kind::date>>(store); break;
        case kind::time_of_day: res = details::count_distinct<runtime_t<kind::time_of_day>>(store); break;
        case kind::time_point: res = details::count_distinct<runtime_t<kind::time_point>>(store); break;
        default: fail_with_exception();
    }
    target.set_value<runtime_t<kind::int8>>(target_offset, res);
}

}  // namespace builtin

}  // namespace jogasaki::executor::function
