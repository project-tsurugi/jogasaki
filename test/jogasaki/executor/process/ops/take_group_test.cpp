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
#include <jogasaki/executor/process/impl/ops/take_group.h>

#include <string>

#include <gtest/gtest.h>

#include <takatori/plan/group.h>
#include <takatori/plan/forward.h>
#include <yugawara/binding/factory.h>
#include <yugawara/storage/basic_configurable_provider.h>

#include <jogasaki/test_root.h>
#include <jogasaki/test_utils.h>
#include <jogasaki/executor/process/impl/ops/take_group_context.h>

#include <jogasaki/mock/basic_record.h>
#include <jogasaki/executor/process/mock/task_context.h>

namespace jogasaki::executor::process::impl::ops {

using namespace meta;
using namespace testing;
using namespace executor;
using namespace accessor;
using namespace takatori::util;
using namespace std::string_view_literals;
using namespace std::string_literals;

using namespace jogasaki::memory;
using namespace boost::container::pmr;

namespace relation = ::takatori::relation;
namespace scalar = ::takatori::scalar;
using take = relation::step::take_group;
using buffer = relation::buffer;

namespace storage = yugawara::storage;

class take_group_test : public test_root {};

TEST_F(take_group_test, simple) {
    binding::factory bindings;

    auto&& g0c0 = bindings.exchange_column();
    auto&& g0c1 = bindings.exchange_column();
    auto&& g0c2 = bindings.exchange_column();
    ::takatori::plan::group g0{
        {
            g0c0,
            g0c1,
            g0c2,
        },
        {
            g0c0,
            g0c1,
        },
    };
    takatori::plan::graph_type p;
    auto&& p0 = p.insert(takatori::plan::process {});
    auto c0 = bindings.stream_variable("c0");
    auto c1 = bindings.stream_variable("c1");
    auto c2 = bindings.stream_variable("c2");
    auto& r0 = p0.operators().insert(relation::step::take_group {
        bindings(g0),
        {
            { g0c0, c0 },
            { g0c1, c1 },
            { g0c2, c2 },
        },
    });

    ::takatori::plan::forward f1 {
        bindings.exchange_column(),
        bindings.exchange_column(),
        bindings.exchange_column(),
    };
    auto&& f1c0 = f1.columns()[0];
    auto&& f1c1 = f1.columns()[1];
    auto&& f1c2 = f1.columns()[2];
    // without offer, the columns are not used and block variables become empty
    auto&& r1 = p0.operators().insert(relation::step::offer {
        bindings.exchange(f1),
        {
            { c0, f1c0 },
            { c1, f1c1 },
            { c2, f1c2 },
        },
    });
    r0.output() >> r1.input(); // connection required by takatori

    auto vmap = std::make_shared<yugawara::analyzer::variable_mapping>();
    vmap->bind(g0c0, t::int4{});
    vmap->bind(g0c1, t::float8{});
    vmap->bind(g0c2, t::int8{});
    vmap->bind(c0, t::int4{});
    vmap->bind(c1, t::float8{});
    vmap->bind(c2, t::int8{});
    yugawara::compiled_info c_info{{}, vmap};

    processor_info p_info{p0.operators(), c_info};

    // currently this vector order defines the order of variables
    // TODO fix when the logic is fixed
    std::vector<variable> columns{g0c1, g0c0, g0c2};
    std::vector<variable> keys{g0c1, g0c0};
    variable_order order{
        variable_ordering_enum_tag<variable_ordering_kind::group_from_keys>,
        columns,
        keys,
    };

    std::vector<take_group::column> take_group_columns{
        {g0c0, c0},
        {g0c1, c1},
        {g0c2, c2},
    };
    take_group s{
        p_info, 0,
        order,
        take_group_columns,
        0,
        nullptr
    };

    auto& block_info = p_info.scopes_info()[s.block_index()];
    block_scope variables{block_info};

    using kind = meta::field_type_kind;
    using test_record = jogasaki::mock::basic_record<kind::float8, kind::int4, kind::int8>;
    std::vector<test_record> records{
        test_record{1.0, 10, 100},
        test_record{2.0, 20, 200},
    };
    auto meta = std::make_shared<record_meta>(
        std::vector<field_type>{
            field_type(enum_tag<kind::float8>),
            field_type(enum_tag<kind::int4>),
            field_type(enum_tag<kind::int8>),
        },
        boost::dynamic_bitset<std::uint64_t>{"000"s}
    );
    auto reader = std::make_shared<mock::basic_record_reader<test_record>>(records, meta);

    mock::task_context task_ctx{
        {reader_container{reader.get()}},
        {},
        {},
        {},
    };

    take_group_context ctx(&task_ctx, variables);

    auto vars_ref = variables.store().ref();
    auto map = variables.value_map();
    auto vars_meta = variables.meta();

    auto c0_offset = map.at(c0).value_offset();
    auto c1_offset = map.at(c1).value_offset();
    auto c2_offset = map.at(c2).value_offset();
    s(ctx, nullptr);
    // TODO verify interim results
//    EXPECT_EQ(10, vars_ref.get_value<std::int32_t>(c0_offset));
//    EXPECT_DOUBLE_EQ(1.0, vars_ref.get_value<double>(c1_offset));
//    EXPECT_EQ(100, vars_ref.get_value<std::int64_t>(c2_offset));

//    ASSERT_TRUE(s(ctx));
    EXPECT_EQ(20, vars_ref.get_value<std::int32_t>(c0_offset));
    EXPECT_DOUBLE_EQ(2.0, vars_ref.get_value<double>(c1_offset));
    EXPECT_EQ(200, vars_ref.get_value<std::int64_t>(c2_offset));

//    ASSERT_FALSE(s(ctx));
}

}

