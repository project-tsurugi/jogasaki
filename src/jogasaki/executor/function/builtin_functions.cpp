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
#include "builtin_functions.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <variant>
#include <boost/assert.hpp>
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
#include <jogasaki/executor/function/field_locator.h>
#include <jogasaki/executor/function/value_generator.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/memory/monotonic_paged_memory_resource.h>
#include <jogasaki/memory/page_pool.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_traits.h>
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
    constexpr static std::size_t minimum_aggregate_id = 1000;
    std::size_t id = aggregate::declaration::minimum_builtin_function_id
        + minimum_aggregate_id;

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
        repo.add(id, count_distinct);
        functions.add({
            id++,
            name,
            t::int8(),
            {
                t::boolean(),
            },
            false,
        });
        repo.add(id, count_distinct);
        functions.add({
            id++,
            name,
            t::int8(),
            {
                t::int4(),
            },
            false,
        });
        repo.add(id, count_distinct);
        functions.add({
            id++,
            name,
            t::int8(),
            {
                t::int8(),
            },
            false,
        });
        repo.add(id, count_distinct);
        functions.add({
            id++,
            name,
            t::int8(),
            {
                t::float4(),
            },
        });
        repo.add(id, count_distinct);
        functions.add({
            id++,
            name,
            t::int8(),
            {
                t::float8(),
            },
            false,
        });
        repo.add(id, count_distinct);
        functions.add({
            id++,
            name,
            t::int8(),
            {
                t::character(t::varying),
            },
            false,
        });
        repo.add(id, count_distinct);
        functions.add({
            id++,
            name,
            t::int8(),
            {
                t::octet(t::varying),
            },
            false,
        });
        repo.add(id, count_distinct);
        functions.add({
            id++,
            name,
            t::int8(),
            {
                t::decimal(),
            },
            false,
        });
        repo.add(id, count_distinct);
        functions.add({
            id++,
            name,
            t::int8(),
            {
                t::date(),
            },
            false,
        });
        repo.add(id, count_distinct);
        functions.add({
            id++,
            name,
            t::int8(),
            {
                t::time_of_day(),
            },
            false,
        });
        repo.add(id, count_distinct);
        functions.add({
            id++,
            name,
            t::int8(),
            {
                t::time_of_day(takatori::type::with_time_zone),
            },
            false,
        });
        repo.add(id, count_distinct);
        functions.add({
            id++,
            name,
            t::int8(),
            {
                t::time_point(),
            },
            false,
        });
        repo.add(id, count_distinct);
        functions.add({
            id++,
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
std::int64_t count_distinct(data::value_store const& store) {
    using bucket_type = tsl::detail_hopscotch_hash::hopscotch_bucket<T, 62, false>;
    using hash_table_allocator = boost::container::pmr::polymorphic_allocator<T>;
    using hash_set = tsl::hopscotch_set<T, std::hash<T>, std::equal_to<>, hash_table_allocator>;
    // hopscotch default power_of_two_growth_policy forces the # of buckets to be power of two,
    // so round down here to avoid going over allocator limit
    constexpr static std::size_t default_initial_hash_table_size =
        utils::round_down_to_power_of_two(memory::page_size / sizeof(bucket_type) - 32); // hopscotch has some (~1KB)
                                                                                     // overhead outside bucket storage

    auto b = store.begin<T>();
    auto e = store.end<T>();
    memory::monotonic_paged_memory_resource resource{&global::page_pool()};
    hash_set values{
        default_initial_hash_table_size,
        std::hash<T>{},
        std::equal_to<>{},
        hash_table_allocator{&resource}
    };
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
    BOOST_ASSERT(args.size() == 1);  //NOLINT
    BOOST_ASSERT(target_loc.type().kind() == kind::int8);  //NOLINT
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
