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
#include <jogasaki/executor/process/impl/ops/take_cogroup.h>

#include <gtest/gtest.h>
#include <glog/logging.h>

#include <takatori/plan/forward.h>
#include <takatori/type/character.h>
#include <takatori/value/character.h>
#include <takatori/util/object_creator.h>
#include <takatori/relation/step/take_cogroup.h>
#include <takatori/relation/step/join.h>
#include <yugawara/binding/factory.h>
#include <yugawara/storage/basic_configurable_provider.h>

#include <jogasaki/test_root.h>
#include <jogasaki/test_utils.h>

#include <jogasaki/executor/process/impl/ops/join.h>

#include <jogasaki/executor/process/mock/group_reader.h>
#include <takatori/plan/group.h>
#include <jogasaki/executor/exchange/group/shuffle_info.h>
#include <jogasaki/executor/process/mock/task_context.h>

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
using namespace boost::container::pmr;

namespace relation = ::takatori::relation;
namespace scalar = ::takatori::scalar;

namespace storage = yugawara::storage;

class take_cogroup_test : public test_root {
public:
};

using kind = field_type_kind;
using group_reader = mock::basic_group_reader<jogasaki::mock::basic_record<kind::int8, kind::int4>, jogasaki::mock::basic_record<kind::int8>>;
using group_type = group_reader::group_type;
using keys_type = group_type::key_type;
using values_type = group_type::value_type;

TEST_F(take_cogroup_test, simple) {
    group_reader reader1 {
        {
            group_type{
                keys_type{1, 10},
                {
                    values_type{100},
                    values_type{101},
                },
            },
            group_type{
                keys_type{2, 20},
                {
                    values_type{200},
                },
            },
        }
    };
    group_reader reader2 {
        {
            group_type{
                keys_type{1, 10},
                {
                    values_type{1000},
                    values_type{1001},
                },
            },
            group_type{
                keys_type{3, 30},
                {
                    values_type{300},
                },
            },
        }
    };

    auto meta = test_group_meta1();
    auto key_offset = meta->key().value_offset(0);
    auto value_offset = meta->value().value_offset(0);

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
    auto&& g1c0 = bindings.exchange_column("g1c0");
    auto&& g1c1 = bindings.exchange_column("g1c1");
    auto&& g1c2 = bindings.exchange_column("g1c2");
    ::takatori::plan::group g1{
        {
            g1c0,
            g1c1,
            g1c2,
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
    auto g1v0 = bindings.stream_variable("g1v0");
    auto g1v1 = bindings.stream_variable("g1v1");
    auto g1v2 = bindings.stream_variable("g1v2");

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
                { g1c0, g1v0 },
                { g1c1, g1v1 },
                { g1c2, g1v2 },
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
        bindings.exchange_column("f1g1v0"),
        bindings.exchange_column("f1g1v1"),
        bindings.exchange_column("f1g1v2"),
    };
    // without offer, the columns are not used and block variables become empty
    auto&& r2 = p0.operators().insert(relation::step::offer {
        bindings(f1),
        {
            { g0v0, f1.columns()[0] },
            { g0v1, f1.columns()[1] },
            { g0v2, f1.columns()[2] },
            { g1v0, f1.columns()[3] },
            { g1v1, f1.columns()[4] },
            { g1v2, f1.columns()[5] },
        }
    });
    r1.output() >> r2.input(); // connection required by takatori
    auto vmap = std::make_shared<yugawara::analyzer::variable_mapping>();
    vmap->bind(g0c0, t::int8{});
    vmap->bind(g0c1, t::int4{});
    vmap->bind(g0c2, t::int8{});
    vmap->bind(g1c0, t::int8{});
    vmap->bind(g1c1, t::int4{});
    vmap->bind(g1c2, t::int8{});
    vmap->bind(g0v0, t::int8{});
    vmap->bind(g0v1, t::int4{});
    vmap->bind(g0v2, t::int8{});
    vmap->bind(g1v0, t::int8{});
    vmap->bind(g1v1, t::int4{});
    vmap->bind(g1v2, t::int8{});
    yugawara::compiled_info c_info{{}, vmap};

    processor_info p_info{p0.operators(), c_info};

    std::vector<group_element> groups{};
    meta::variable_order order0{
        variable_ordering_enum_tag<variable_ordering_kind::group_from_keys>,
            g0.columns(),
            g0.group_keys()
    };
    meta::group_meta gmeta{};
    meta::variable_order order1{
        variable_ordering_enum_tag<variable_ordering_kind::group_from_keys>,
        g1.columns(),
        g1.group_keys()
    };
    auto input_meta = std::make_shared<record_meta>(
        std::vector<field_type>{
            field_type(enum_tag<kind::int8>),
            field_type(enum_tag<kind::int4>),
            field_type(enum_tag<kind::int8>),
        },
        boost::dynamic_bitset<std::uint64_t>{"000"s}
    );
    exchange::group::shuffle_info s_info{input_meta, {0,1}};
    auto key_meta = s_info.key_meta();
    auto value_meta = s_info.value_meta();
    auto& block_info = p_info.scopes_info()[0];
    block_scope variables{block_info};

    groups.emplace_back(
        order0,
        s_info.group_meta(),
        r0.groups()[0].columns(),
        0,
        block_info
    );
    groups.emplace_back(
        order1,
        s_info.group_meta(),
        r0.groups()[1].columns(),
        1,
        block_info
    );

    auto d = std::make_unique<cogroup_verifier>();
    auto downstream = d.get();
    take_cogroup cgrp{
        0,
        p_info,
        0,
        groups,
        std::move(d)
    };

    mock::task_context task_ctx{
        {
            reader_container{&reader1},
            reader_container{&reader2}
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
        key_meta,
        &resource,
        &varlen_resource
    );

    auto vars_ref = variables.store().ref();
    auto map = variables.value_map();
    auto vars_meta = variables.meta();

    auto g0v0_offset = map.at(g0v0).value_offset();
    auto g0v1_offset = map.at(g0v1).value_offset();
    auto g0v2_offset = map.at(g0v2).value_offset();
    auto g1v0_offset = map.at(g1v0).value_offset();
    auto g1v1_offset = map.at(g1v1).value_offset();
    auto g1v2_offset = map.at(g1v2).value_offset();

    std::size_t count = 0;
    auto keys_type_meta = keys_type{}.record_meta();
    auto values_type_meta = values_type{}.record_meta();

    downstream->body([&](cogroup& c) {
        ASSERT_EQ(2, c.groups().size());
        comparator comp{key_meta.get()};
        auto value_size = value_meta->record_size();
        switch(count) {
            case 0: {
                auto& g = c.groups()[0];
                auto b = g.begin();
                ASSERT_NE(g.end(), b);
                values_type v1{record_ref{*b, value_size}, values_type_meta};
                EXPECT_EQ(values_type{100}, v1);
                ++b;
                ASSERT_NE(g.end(), b);
                values_type v2{record_ref{*b, value_size}, values_type_meta};
                EXPECT_EQ(values_type{101}, v2);
                ++b;
                EXPECT_EQ(g.end(), b);
                break;
            }
            case 1: {
                auto& g = c.groups()[0];
                auto b = g.begin();
                ASSERT_NE(g.end(), b);
                values_type v1{record_ref{*b, value_size}, values_type_meta};
                EXPECT_EQ(values_type{200}, v1);
                ++b;
                EXPECT_EQ(g.end(), b);
                break;
            }
            case 2: {
                auto& g = c.groups()[0];
                auto b = g.begin();
                EXPECT_EQ(g.end(), b);
                EXPECT_TRUE(g.empty());
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

