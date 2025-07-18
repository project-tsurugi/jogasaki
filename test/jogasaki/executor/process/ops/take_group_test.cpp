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
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <boost/container/container_fwd.hpp>
#include <boost/cstdint.hpp>
#include <boost/dynamic_bitset/dynamic_bitset.hpp>
#include <boost/move/utility_core.hpp>
#include <gtest/gtest.h>

#include <takatori/descriptor/variable.h>
#include <takatori/graph/graph.h>
#include <takatori/graph/port.h>
#include <takatori/plan/forward.h>
#include <takatori/plan/graph.h>
#include <takatori/plan/group.h>
#include <takatori/plan/process.h>
#include <takatori/relation/expression.h>
#include <takatori/relation/expression_kind.h>
#include <takatori/relation/step/offer.h>
#include <takatori/scalar/expression_kind.h>
#include <takatori/type/primitive.h>
#include <takatori/util/infect_qualifier.h>
#include <yugawara/analyzer/variable_mapping.h>
#include <yugawara/binding/factory.h>
#include <yugawara/compiled_info.h>
#include <yugawara/storage/sequence.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/accessor/text.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/executor/exchange/group/group_info.h>
#include <jogasaki/executor/io/reader_container.h>
#include <jogasaki/executor/process/impl/ops/operator_base.h>
#include <jogasaki/executor/process/impl/ops/take_group.h>
#include <jogasaki/executor/process/impl/ops/take_group_context.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/executor/process/mock/group_reader.h>
#include <jogasaki/executor/process/mock/task_context.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/memory/page_pool.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/group_meta.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/meta/variable_order.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/test_root.h>
#include <jogasaki/test_utils.h>

#include "verifier.h"

namespace jogasaki::executor::process::impl::ops {

using namespace meta;
using namespace testing;
using namespace executor;
using namespace accessor;
using namespace takatori::util;
using namespace std::string_view_literals;
using namespace std::string_literals;

using namespace jogasaki::memory;
using namespace jogasaki::mock;
using namespace boost::container::pmr;

namespace relation = ::takatori::relation;
namespace scalar = ::takatori::scalar;

namespace storage = yugawara::storage;

using namespace jogasaki::executor::exchange::group;

class take_group_test : public test_root {
public:

    basic_record create_key(
        double arg0,
        std::int32_t arg1
    ) {
        return create_nullable_record<kind::float8, kind::int4>(arg0, arg1);
    }

    basic_record create_value(
        std::int64_t arg0
    ) {
        return create_nullable_record<kind::int8>(arg0);
    }
};

TEST_F(take_group_test, simple) {
    binding::factory bindings;

    auto&& g0c0 = bindings.exchange_column("g0c0");
    auto&& g0c1 = bindings.exchange_column("g0c1");
    auto&& g0c2 = bindings.exchange_column("g0c2");
    ::takatori::plan::group g0{
        {
            g0c0,
            g0c1,
            g0c2,
        },
        {
            g0c1,
            g0c0,
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
        bindings.exchange_column("f1c0"),
        bindings.exchange_column("f1c1"),
        bindings.exchange_column("f1c2"),
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
    using kind = meta::field_type_kind;

    using key_record = jogasaki::mock::basic_record;
    using value_record = jogasaki::mock::basic_record;
    using reader = mock::basic_group_reader;
    using group_type = reader::group_type;
    using groups_type = reader::groups_type;
    auto input_meta = std::make_shared<record_meta>(
        std::vector<field_type>{
            field_type(field_enum_tag<kind::float8>),
            field_type(field_enum_tag<kind::int4>),
            field_type(field_enum_tag<kind::int8>),
        },
        boost::dynamic_bitset<std::uint64_t>{3}.flip()
    );
    group_info s_info{input_meta, {0,1}};

    auto d = std::make_unique<group_verifier>();
    auto downstream = d.get();
    take_group s{
        0,
        p_info,
        0,
        order,
        s_info.group_meta(),
        take_group_columns,
        0,
        std::move(d)
    };

    auto& block_info = p_info.vars_info_list()[s.block_index()];
    variable_table variables{block_info};
    groups_type groups{
        group_type{
            create_key(1.0, 10),
            {
                create_value (100),
                create_value (200),
            },
        },
        group_type{
            create_key(2.0, 20),
                {
                    create_value (100),
                    create_value (200),
                },
        }
    };
    auto internal_meta = group_meta(
        groups[0].key().record_meta(),
        groups[0].values()[0].record_meta()
    );
    auto r = std::make_shared<reader>(
        groups,
        maybe_shared_ptr(&internal_meta),
        s_info.group_meta()
    );
    mock::task_context task_ctx{
        {io::reader_container{r.get()}},
        {},
        {},
        {},
    };

    memory::page_pool pool{};
    memory::lifo_paged_memory_resource resource{&pool};
    take_group_context ctx(&task_ctx, variables, nullptr, &resource);

    auto vars_ref = variables.store().ref();
    auto map = variables.info();
    auto vars_meta = variables.meta();

    auto c0_offset = map.at(c0).value_offset();
    auto c1_offset = map.at(c1).value_offset();
    auto c2_offset = map.at(c2).value_offset();

    std::size_t count = 0;
    downstream->body([&](bool last_member) {
        switch(count) {
            case 0: {
                EXPECT_FALSE(last_member);
                EXPECT_EQ(10, vars_ref.get_value<std::int32_t>(c0_offset));
                EXPECT_DOUBLE_EQ(1.0, vars_ref.get_value<double>(c1_offset));
                EXPECT_EQ(100, vars_ref.get_value<std::int64_t>(c2_offset));
                break;
            }
            case 1: {
                EXPECT_TRUE(last_member);
                EXPECT_EQ(10, vars_ref.get_value<std::int32_t>(c0_offset));
                EXPECT_DOUBLE_EQ(1.0, vars_ref.get_value<double>(c1_offset));
                EXPECT_EQ(200, vars_ref.get_value<std::int64_t>(c2_offset));
                break;
            }
            case 2: {
                EXPECT_FALSE(last_member);
                EXPECT_EQ(20, vars_ref.get_value<std::int32_t>(c0_offset));
                EXPECT_DOUBLE_EQ(2.0, vars_ref.get_value<double>(c1_offset));
                EXPECT_EQ(100, vars_ref.get_value<std::int64_t>(c2_offset));
                break;
            }
            case 3: {
                EXPECT_TRUE(last_member);
                EXPECT_EQ(20, vars_ref.get_value<std::int32_t>(c0_offset));
                EXPECT_DOUBLE_EQ(2.0, vars_ref.get_value<double>(c1_offset));
                EXPECT_EQ(200, vars_ref.get_value<std::int64_t>(c2_offset));
                break;
            }
            default:
                ADD_FAILURE();
        }
        ++count;
    });
    s(ctx);
    ASSERT_EQ(4, count);
    ctx.release();
}

}

