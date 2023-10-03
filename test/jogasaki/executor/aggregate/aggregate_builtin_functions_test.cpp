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
#include <jogasaki/executor/function/builtin_functions.h>

#include <gtest/gtest.h>
#include <boost/dynamic_bitset.hpp>

#include <jogasaki/accessor/text.h>
#include <jogasaki/data/value_store.h>
#include <jogasaki/memory/page_pool.h>
#include <jogasaki/memory/monotonic_paged_memory_resource.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/exchange/aggregate/aggregate_info.h>
#include <jogasaki/executor/function/aggregate_function_repository.h>
#include <jogasaki/executor/function/builtin_functions.h>
#include <jogasaki/mock/basic_record.h>

namespace jogasaki::executor::function {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace meta;
using namespace executor;
using namespace takatori::util;

class aggregate_builtin_functions_test : public ::testing::Test {};

using kind = meta::field_type_kind;
using accessor::text;

TEST_F(aggregate_builtin_functions_test, count_distinct_int4) {
    memory::page_pool pool{};
    memory::monotonic_paged_memory_resource resource{&pool};
    memory::monotonic_paged_memory_resource varlen_resource{&pool};
    data::value_store store{
        meta::field_type{meta::field_enum_tag<kind::int4>},
        &resource,
        &varlen_resource
    };

    store.append<std::int32_t>(1);
    store.append<std::int32_t>(2);
    store.append<std::int32_t>(3);
    store.append<std::int32_t>(2);
    store.append<std::int32_t>(3);
    store.append<std::int32_t>(4);

    mock::basic_record target{mock::create_nullable_record<kind::int8>()};
    std::vector<std::reference_wrapper<data::value_store>> args{};
    args.emplace_back(store);
    auto meta = target.record_meta();
    builtin::count_distinct(
        target.ref(),
        field_locator{
            meta->at(0),
            true,
            meta->value_offset(0),
            meta->nullity_offset(0),
        },
        args
    );
    ASSERT_EQ(4, target.ref().get_value<std::int64_t>(meta->value_offset(0)));
}

TEST_F(aggregate_builtin_functions_test, count_distinct_character) {
    memory::page_pool pool{};
    memory::monotonic_paged_memory_resource resource{&pool};
    memory::monotonic_paged_memory_resource varlen_resource{&pool};
    data::value_store store{
        meta::field_type{meta::field_enum_tag<kind::character>},
        &resource,
        &varlen_resource
    };

    store.append<text>(text{"A"});
    store.append<text>(text{"BB"});
    store.append<text>(text{"CCC"});
    store.append<text>(text{"AAA"});
    store.append<text>(text{"AA"});
    store.append<text>(text{"A"});

    mock::basic_record target{mock::create_nullable_record<kind::int8>()};
    std::vector<std::reference_wrapper<data::value_store>> args{};
    args.emplace_back(store);
    auto meta = target.record_meta();
    builtin::count_distinct(
        target.ref(),
        field_locator{
            meta->at(0),
            true,
            meta->value_offset(0),
            meta->nullity_offset(0),
        },
        args
    );
    ASSERT_EQ(5, target.ref().get_value<std::int64_t>(meta->value_offset(0)));
}

}

