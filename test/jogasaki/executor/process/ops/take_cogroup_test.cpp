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

#include <takatori/graph/graph.h>
#include <takatori/graph/port.h>
#include <takatori/plan/forward.h>
#include <takatori/plan/graph.h>
#include <takatori/plan/group.h>
#include <takatori/plan/process.h>
#include <takatori/relation/expression.h>
#include <takatori/relation/expression_kind.h>
#include <takatori/relation/step/join.h>
#include <takatori/relation/step/offer.h>
#include <takatori/relation/step/take_cogroup.h>
#include <takatori/scalar/expression_kind.h>
#include <takatori/type/primitive.h>
#include <takatori/util/fail.h>
#include <yugawara/analyzer/variable_mapping.h>
#include <yugawara/binding/factory.h>
#include <yugawara/compiled_info.h>
#include <yugawara/storage/sequence.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/data/iterable_record_store.h>
#include <jogasaki/executor/common/port.h>
#include <jogasaki/executor/exchange/group/group_info.h>
#include <jogasaki/executor/io/reader_container.h>
#include <jogasaki/executor/process/impl/ops/operator_base.h>
#include <jogasaki/executor/process/impl/ops/take_cogroup.h>
#include <jogasaki/executor/process/impl/ops/take_cogroup_context.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/mock/group_reader.h>
#include <jogasaki/executor/process/mock/task_context.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/memory/page_pool.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/group_meta.h>
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

class take_cogroup_test : public test_root {
public:
    basic_record key0(
        std::int64_t arg0,
        std::int32_t arg1
    ) {
        return create_nullable_record<kind::int8, kind::int4>(arg0, arg1);
    }

    basic_record value0(
        double arg0
    ) {
        return create_nullable_record<kind::float8>(arg0);
    }
    basic_record key1(
        std::int64_t arg0,
        std::int32_t arg1
    ) {
        return create_nullable_record<kind::int8, kind::int4>(arg0, arg1);
    }

    basic_record value1(
        float arg0
    ) {
        return create_nullable_record<kind::float4>(arg0);
    }
};

using kind = field_type_kind;
using group_reader = mock::basic_group_reader;
using group_type = group_reader::group_type;
using keys_type = group_type::key_type;
using values_type = group_type::value_type;

TEST_F(take_cogroup_test, simple) {

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
            g0c0,
            g0c1,
        },
    };
    auto&& g1c2 = bindings.exchange_column("g1c2");
    auto&& g1c1 = bindings.exchange_column("g1c1");
    auto&& g1c0 = bindings.exchange_column("g1c0");
    ::takatori::plan::group g1{
        {
            g1c2,
            g1c1,
            g1c0,
        },
        {
            g1c0,
            g1c1,
        },
    };
    takatori::plan::graph_type p;
    auto&& p0 = p.insert(takatori::plan::process {});
    auto g0v0 = bindings.stream_variable("g0v0");
    auto g0v1 = bindings.stream_variable("g0v1");
    auto g0v2 = bindings.stream_variable("g0v2");
    auto g1v2 = bindings.stream_variable("g1v2");
    auto g1v1 = bindings.stream_variable("g1v1");
    auto g1v0 = bindings.stream_variable("g1v0");

    auto& r0 = p0.operators().insert(relation::step::take_cogroup {
        {
            bindings(g0),
            {
                { g0c0, g0v0 },
                { g0c1, g0v1 },
                { g0c2, g0v2 },
            },
        },
        {
            bindings(g1),
            {
                { g1c2, g1v2 },
                { g1c1, g1v1 },
                { g1c0, g1v0 },
            },
        }
    });

    auto&& r1 = p0.operators().insert(relation::step::join {
        relation::step::join::operator_kind_type::inner
    });
    r0.output() >> r1.input();
    ::takatori::plan::forward f1 {
        bindings.exchange_column("f1g0v0"),
        bindings.exchange_column("f1g0v1"),
        bindings.exchange_column("f1g0v2"),
        bindings.exchange_column("f1g1v2"),
        bindings.exchange_column("f1g1v1"),
        bindings.exchange_column("f1g1v0"),
    };
    // without offer, the columns are not used and block variables become empty
    auto&& r2 = p0.operators().insert(relation::step::offer {
        bindings(f1),
        {
            { g0v0, f1.columns()[0] },
            { g0v1, f1.columns()[1] },
            { g0v2, f1.columns()[2] },
            { g1v2, f1.columns()[3] },
            { g1v1, f1.columns()[4] },
            { g1v0, f1.columns()[5] },
        }
    });
    r1.output() >> r2.input(); // connection required by takatori
    auto vmap = std::make_shared<yugawara::analyzer::variable_mapping>();
    vmap->bind(g0c0, t::int8{});
    vmap->bind(g0c1, t::int4{});
    vmap->bind(g0c2, t::float8{});
    vmap->bind(g1c2, t::float4{});
    vmap->bind(g1c1, t::int4{});
    vmap->bind(g1c0, t::int8{});
    vmap->bind(g0v0, t::int8{});
    vmap->bind(g0v1, t::int4{});
    vmap->bind(g0v2, t::float8{});
    vmap->bind(g1v2, t::float4{});
    vmap->bind(g1v1, t::int4{});
    vmap->bind(g1v0, t::int8{});
    yugawara::compiled_info c_info{{}, vmap};

    processor_info p_info{p0.operators(), c_info};

    meta::variable_order order0{
        variable_ordering_enum_tag<variable_ordering_kind::group_from_keys>,
            g0.columns(),
            g0.group_keys()
    };
    meta::variable_order order1{
        variable_ordering_enum_tag<variable_ordering_kind::group_from_keys>,
        g1.columns(),
        g1.group_keys()
    };
    auto input_meta0 = std::make_shared<record_meta>(
        std::vector<field_type>{
            field_type(field_enum_tag<kind::int8>),
            field_type(field_enum_tag<kind::int4>),
            field_type(field_enum_tag<kind::float8>),
        },
        boost::dynamic_bitset<std::uint64_t>{3}.flip()
    );
    auto input_meta1 = std::make_shared<record_meta>(
        std::vector<field_type>{
            field_type(field_enum_tag<kind::float4>),
            field_type(field_enum_tag<kind::int4>),
            field_type(field_enum_tag<kind::int8>),
        },
        boost::dynamic_bitset<std::uint64_t>{3}.flip()
    );
    exchange::group::group_info g_info0{input_meta0, {0,1}};
    auto key_meta0 = g_info0.key_meta();
    auto value_meta0 = g_info0.value_meta();
    exchange::group::group_info g_info1{input_meta1, {2,1}};
    auto key_meta1 = g_info1.key_meta();
    auto value_meta1 = g_info1.value_meta();

    auto& block_info = p_info.vars_info_list()[0];
    variable_table variables{block_info};

    std::vector<group_element> groups{};
    groups.emplace_back(
        order0,
        g_info0.group_meta(),
        r0.groups()[0].columns(),
        0,
        block_info
    );
    groups.emplace_back(
        order1,
        g_info1.group_meta(),
        r0.groups()[1].columns(),
        1,
        block_info
    );

    using iterator = data::iterable_record_store::iterator;
    auto d = std::make_unique<cogroup_verifier<iterator>>();
    auto downstream = d.get();
    take_cogroup cgrp{
        0,
        p_info,
        0,
        groups,
        std::move(d)
    };
    auto k0 = key0(0, 0);
    auto v0 = value0(0);
    auto internal_meta0 = group_meta(
        k0.record_meta(),
        v0.record_meta()
    );
    auto k1 = key1(0, 0);
    auto v1 = value1(0);
    auto internal_meta1 = group_meta(
        k1.record_meta(),
        v1.record_meta()
    );

    group_reader reader0 {
        {
            group_type{
                key0(1, 10),
                {
                    value0(100),
                    value0(101),
                },
            },
            group_type{
                key0(2, 20),
                {
                    value0(200),
                },
            },
        },
        maybe_shared_ptr(&internal_meta0),
        g_info0.group_meta()
    };
    group_reader reader1 {
        {
            group_type{
                key1(1, 10),
                {
                    value1(1000),
                    value1(1001),
                },
            },
            group_type{
                key1(3, 30),
                {
                    value1(300),
                },
            },
        },
        maybe_shared_ptr(&internal_meta1),
        g_info1.group_meta()
    };
    mock::task_context task_ctx{
        {
            io::reader_container{&reader0},
            io::reader_container{&reader1}
        },
        {},
        {},
        {},
    };

    memory::page_pool pool{};
    memory::lifo_paged_memory_resource resource{&pool};
    memory::lifo_paged_memory_resource varlen_resource{&pool};
    take_cogroup_context ctx(
        &task_ctx,
        variables,
        &resource,
        &varlen_resource
    );

    std::size_t count = 0;

    downstream->body([&](cogroup<iterator>& c) {
        ASSERT_EQ(2, c.groups().size());
        switch(count) {
            case 0: {
                {
                    auto& g0 = c.groups()[0];
                    keys_type k1{g0.key(), key_meta0};
                    EXPECT_EQ(key0(1,10), k1);
                    auto b = g0.begin();
                    ASSERT_NE(g0.end(), b);
                    values_type v1{*b, value_meta0};
                    EXPECT_EQ(value0(100), v1);
                    ++b;
                    ASSERT_NE(g0.end(), b);
                    values_type v2{*b, value_meta0};
                    EXPECT_EQ(value0(101), v2);
                    ++b;
                    EXPECT_EQ(g0.end(), b);
                }
                {
                    auto& g1 = c.groups()[1];
                    keys_type k1{g1.key(), key_meta1};
                    EXPECT_EQ(key1(1,10), k1);
                    auto b = g1.begin();
                    ASSERT_NE(g1.end(), b);
                    values_type v1{*b, value_meta1};
                    EXPECT_EQ(value1(1000), v1);
                    ++b;
                    ASSERT_NE(g1.end(), b);
                    values_type v2{*b, value_meta1};
                    EXPECT_EQ(value1(1001), v2);
                    ++b;
                    EXPECT_EQ(g1.end(), b);
                }
                break;
            }
            case 1: {
                {
                    auto& g = c.groups()[0];
                    keys_type k0{g.key(), key_meta0};
                    EXPECT_EQ(key0(2,20), k0);
                    auto b = g.begin();
                    ASSERT_NE(g.end(), b);
                    values_type v1{*b, value_meta0};
                    EXPECT_EQ(value0(200), v1);
                    ++b;
                    EXPECT_EQ(g.end(), b);
                }
                {
                    auto& g1 = c.groups()[1];
                    auto b = g1.begin();
                    EXPECT_EQ(g1.end(), b);
                    EXPECT_TRUE(g1.empty());
                }
                break;
            }
            case 2: {
                {
                    auto& g0 = c.groups()[0];
                    auto b = g0.begin();
                    EXPECT_EQ(g0.end(), b);
                    EXPECT_TRUE(g0.empty());
                }
                {
                    auto& g1= c.groups()[1];
                    keys_type k1{g1.key(), key_meta1};
                    EXPECT_EQ(key1(3,30), k1);
                    auto b = g1.begin();
                    ASSERT_NE(g1.end(), b);
                    values_type v1{*b, value_meta1};
                    EXPECT_EQ(value1(300), v1);
                    ++b;
                    EXPECT_EQ(g1.end(), b);
                }
                break;
            }
            default:
                ADD_FAILURE();
        }
        ++count;
    });
    cgrp(ctx);

    ASSERT_EQ(3, count);
    ctx.release();
}

}

