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
#include <type_traits>
#include <boost/move/utility_core.hpp>
#include <gtest/gtest.h>

#include <takatori/graph/port.h>
#include <takatori/plan/forward.h>
#include <takatori/relation/buffer.h>
#include <takatori/relation/filter.h>
#include <takatori/relation/project.h>
#include <takatori/relation/step/offer.h>
#include <takatori/relation/step/take_flat.h>
#include <takatori/scalar/expression_kind.h>
#include <takatori/scalar/immediate.h>
#include <takatori/statement/statement_kind.h>
#include <takatori/tree/tree_element_vector.h>
#include <takatori/type/primitive.h>
#include <takatori/type/type_kind.h>
#include <takatori/util/fail.h>
#include <takatori/util/rvalue_reference_wrapper.h>
#include <takatori/value/primitive.h>
#include <takatori/value/value_kind.h>
#include <yugawara/analyzer/expression_mapping.h>
#include <yugawara/analyzer/variable_mapping.h>
#include <yugawara/binding/factory.h>
#include <yugawara/storage/configurable_provider.h>
#include <yugawara/storage/table.h>

#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/test_root.h>
#include <jogasaki/test_utils.h>

namespace jogasaki::executor::process::impl {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace meta;
using namespace takatori::util;
using namespace yugawara::binding;

using namespace testing;

namespace type = ::takatori::type;
namespace value = ::takatori::value;
namespace scalar = ::takatori::scalar;
namespace relation = ::takatori::relation;
namespace statement = ::takatori::statement;

using take = relation::step::take_flat;
using offer = relation::step::offer;
using buffer = relation::buffer;
using filter = relation::filter;

using rgraph = ::takatori::relation::graph_type;
using kind = meta::field_type_kind;
class variable_table_info_test : public test_root {

};

TEST_F(variable_table_info_test, basic) {
    factory f;
    auto v1 = f.stream_variable("v1");
    auto v2 = f.exchange_column("v2");
    variable_table_info::entity_type map{};
    map[v1] = value_info{1, 1, 0};
    map[v2] = value_info{2,2, 1};

    auto rec = mock::create_nullable_record<kind::int1, kind::int1>();
    variable_table_info m{std::move(map), rec.record_meta()};
    auto& i1 = m.at(v1);
    ASSERT_EQ(1, i1.value_offset());
    auto& i2 = m.at(v2);
    ASSERT_EQ(2, i2.value_offset());
}

TEST_F(variable_table_info_test, table_column) {
    yugawara::storage::configurable_provider storages_;
    auto t1 = storages_.add_table({
        "T1",
        {
            { "C1", t::int4() },
        },
    });
    auto&& cols = t1->columns();

    factory f;
    auto v1 = f.stream_variable("v1");
    auto v2 = f.table_column(cols[0]);
    variable_table_info::entity_type map{};
    map[v1] = value_info{1, 1, 0};
    map[v2] = value_info{2, 2, 1};

    auto rec = mock::create_nullable_record<kind::int1, kind::int1>();
    variable_table_info m{std::move(map), rec.record_meta()};
    auto& i1 = m.at(v1);
    ASSERT_EQ(1, i1.value_offset());
    auto& i2 = m.at(v2);
    ASSERT_EQ(2, i2.value_offset());
}

TEST_F(variable_table_info_test, create_block_variables_definition1) {
    factory f;
    ::takatori::plan::forward f1 {
        f.exchange_column(),
        f.exchange_column(),
        f.exchange_column(),
    };
    ::takatori::plan::forward f2 {
        f.exchange_column(),
        f.exchange_column(),
        f.exchange_column(),
    };

    rgraph rg;

    auto&& c0 = f.stream_variable("c0");
    auto&& c1 = f.stream_variable("c1");
    auto&& c2 = f.stream_variable("c2");
    auto&& r1 = rg.insert(take {
        f.exchange(f1),
        {
            { f1.columns()[0], c0 },
            { f1.columns()[1], c1 },
            { f1.columns()[2], c2 },
        },
    });
    auto&& fi = rg.insert(
        filter(scalar::immediate{value::boolean{true}, type::boolean{}})
    );
    auto&& r2 = rg.insert(offer {
        f.exchange(f2),
        {
            { c1, f2.columns()[0] },
            { c0, f2.columns()[1] },
            { c0, f2.columns()[2] },
        },
    });
    r1.output() >> fi.input();
    fi.output() >> r2.input();

    auto expression_mapping = std::make_shared<yugawara::analyzer::expression_mapping>();
    auto variable_mapping = std::make_shared<yugawara::analyzer::variable_mapping>();
    variable_mapping->bind(c0, type::int8{});
    variable_mapping->bind(c1, type::int8{});
    variable_mapping->bind(c2, type::int8{});

    yugawara::compiled_info info{expression_mapping, variable_mapping};

    auto pinfo = std::make_shared<processor_info>(rg, info);
    auto p = create_block_variables_definition(pinfo->relations(), pinfo->compiled_info());
    auto& infos = *p.first;
    auto& inds = *p.second;

    ASSERT_EQ(1, infos.size());
    auto meta = infos[0].meta();
    ASSERT_EQ(2, meta->field_count());

    auto& map = infos[0];
    EXPECT_TRUE(map.exists(c0));
    EXPECT_TRUE(map.exists(c1));

    ASSERT_EQ(3, inds.size());
    for(auto&& ind : inds) {
        EXPECT_EQ(0, ind.second);
    }
}

// Tests for buffer (multi-block) support

TEST_F(variable_table_info_test, value_info_block_index) {
    value_info vi{10, 20, 3, std::size_t{5}};
    EXPECT_EQ(10, vi.value_offset());
    EXPECT_EQ(20, vi.nullity_offset());
    EXPECT_EQ(3, vi.index());
    EXPECT_TRUE(vi.block_index().has_value());
    EXPECT_EQ(5u, vi.block_index().value());

    // 3-arg construction leaves block_index as nullopt
    value_info vi2{10, 20, 3};
    EXPECT_FALSE(vi2.block_index().has_value());
}

TEST_F(variable_table_info_test, variable_table_info_parent_delegation) {
    factory f;
    auto c0 = f.stream_variable("c0");
    auto c1 = f.stream_variable("c1");
    auto d0 = f.stream_variable("d0");  // new variable in child block

    // Block 0 info: has c0 (block_index=0) and c1 (block_index=0)
    variable_table_info::entity_type map0{};
    map0[c0] = value_info{0, 0, 0, std::size_t{0}};
    map0[c1] = value_info{8, 1, 1, std::size_t{0}};
    auto meta0 = mock::create_nullable_record<kind::int8, kind::int8>().record_meta();
    variable_table_info info0{std::move(map0), meta0};

    // Block 1 info: has d0 (block_index=1), parent = block 0
    variable_table_info::entity_type map1{};
    map1[d0] = value_info{0, 0, 0, std::size_t{1}};
    auto meta1 = mock::create_nullable_record<kind::int8>().record_meta();
    variable_table_info info1{std::move(map1), meta1, &info0};

    // exists() delegates to parent
    EXPECT_TRUE(info1.exists(c0));   // via parent
    EXPECT_TRUE(info1.exists(c1));   // via parent
    EXPECT_TRUE(info1.exists(d0));   // local

    // exists_local() does NOT delegate
    EXPECT_FALSE(info1.exists_local(c0));
    EXPECT_FALSE(info1.exists_local(c1));
    EXPECT_TRUE(info1.exists_local(d0));

    // at() for parent variable returns block_index=0
    EXPECT_EQ(0u, info1.at(c0).block_index().value());
    EXPECT_EQ(0u, info1.at(c1).block_index().value());

    // at() for local variable returns block_index=1
    EXPECT_EQ(1u, info1.at(d0).block_index().value());
}

TEST_F(variable_table_info_test, create_block_variables_definition_with_buffer) {
    // Graph: take_flat(c0, c1) -> buffer(2) -> [offer0(c0, c1), offer1(c0, c1)]
    factory f;
    ::takatori::plan::forward f1 {
        f.exchange_column(),
        f.exchange_column(),
    };
    ::takatori::plan::forward f2 {
        f.exchange_column(),
        f.exchange_column(),
    };
    ::takatori::plan::forward f3 {
        f.exchange_column(),
        f.exchange_column(),
    };

    rgraph rg;

    auto&& c0 = f.stream_variable("c0");
    auto&& c1 = f.stream_variable("c1");

    auto&& r1 = rg.insert(take {
        f.exchange(f1),
        {
            { f1.columns()[0], c0 },
            { f1.columns()[1], c1 },
        },
    });
    auto&& buf = rg.insert(buffer{2});
    auto&& r2 = rg.insert(offer {
        f.exchange(f2),
        {
            { c0, f2.columns()[0] },
            { c1, f2.columns()[1] },
        },
    });
    auto&& r3 = rg.insert(offer {
        f.exchange(f3),
        {
            { c0, f3.columns()[0] },
            { c1, f3.columns()[1] },
        },
    });
    r1.output() >> buf.input();
    buf.output_ports()[0] >> r2.input();
    buf.output_ports()[1] >> r3.input();

    auto expression_mapping = std::make_shared<yugawara::analyzer::expression_mapping>();
    auto variable_mapping = std::make_shared<yugawara::analyzer::variable_mapping>();
    variable_mapping->bind(c0, type::int8{});
    variable_mapping->bind(c1, type::int8{});

    yugawara::compiled_info info{expression_mapping, variable_mapping};

    auto pinfo = std::make_shared<processor_info>(rg, info);
    auto p = create_block_variables_definition(pinfo->relations(), pinfo->compiled_info());
    auto& infos = *p.first;
    auto& inds = *p.second;

    // 3 blocks: block0 (take+buffer), block1 (offer0), block2 (offer1)
    ASSERT_EQ(3, infos.size());

    // block0: defines c0, c1 (neither killed)
    EXPECT_EQ(2, infos[0].meta()->field_count());
    EXPECT_TRUE(infos[0].exists_local(c0));
    EXPECT_TRUE(infos[0].exists_local(c1));
    EXPECT_EQ(0u, infos[0].at(c0).block_index().value());
    EXPECT_EQ(0u, infos[0].at(c1).block_index().value());

    // block1, block2: no local variables (offer uses but doesn't define)
    EXPECT_EQ(0, infos[1].meta()->field_count());
    EXPECT_EQ(0, infos[2].meta()->field_count());

    // block1 and block2 delegate to parent (block0) for c0, c1
    EXPECT_FALSE(infos[1].exists_local(c0));
    EXPECT_TRUE(infos[1].exists(c0));
    EXPECT_EQ(0u, infos[1].at(c0).block_index().value());  // resolved in block 0
    EXPECT_EQ(0u, infos[2].at(c1).block_index().value());

    // block_indices: take and buf belong to block 0, offers belong to 1 and 2
    EXPECT_EQ(0, inds.at(&r1));
    EXPECT_EQ(0, inds.at(&buf));

    // offer0 must be in block 1 or 2, offer1 in the other
    auto idx_r2 = inds.at(&r2);
    auto idx_r3 = inds.at(&r3);
    EXPECT_NE(idx_r2, idx_r3);
    EXPECT_TRUE(idx_r2 == 1 || idx_r2 == 2);
    EXPECT_TRUE(idx_r3 == 1 || idx_r3 == 2);
}

// Graph: take_flat(c0, c1) -> buffer(2)
//   -> (downstream 0) project(defines d0) -> offer0
//   -> (downstream 1) offer1
// Verifies that d0 is visible in downstream 0's block but not in downstream 1's block.
TEST_F(variable_table_info_test, create_block_variables_definition_downstream_local_var) {
    factory f;
    ::takatori::plan::forward f1 { f.exchange_column(), f.exchange_column() };
    ::takatori::plan::forward f2 { f.exchange_column(), f.exchange_column(), f.exchange_column() };
    ::takatori::plan::forward f3 { f.exchange_column(), f.exchange_column() };

    rgraph rg;

    auto&& c0 = f.stream_variable("c0");
    auto&& c1 = f.stream_variable("c1");
    auto&& d0 = f.stream_variable("d0");  // defined only in downstream 0

    auto&& r1 = rg.insert(take {
        f.exchange(f1),
        { { f1.columns()[0], c0 }, { f1.columns()[1], c1 } },
    });
    auto&& buf = rg.insert(buffer{2});

    using pcolumn = relation::project::column;
    using rref = takatori::util::rvalue_reference_wrapper<pcolumn>;
    auto&& proj = rg.insert(relation::project{
        std::initializer_list<rref>{ pcolumn{d0, scalar::immediate{value::int8{42}, type::int8{}}} }
    });

    auto&& r2 = rg.insert(offer {
        f.exchange(f2),
        { { c0, f2.columns()[0] }, { c1, f2.columns()[1] }, { d0, f2.columns()[2] } },
    });
    auto&& r3 = rg.insert(offer {
        f.exchange(f3),
        { { c0, f3.columns()[0] }, { c1, f3.columns()[1] } },
    });

    r1.output() >> buf.input();
    buf.output_ports()[0] >> proj.input();
    proj.output() >> r2.input();
    buf.output_ports()[1] >> r3.input();

    auto expression_mapping = std::make_shared<yugawara::analyzer::expression_mapping>();
    auto variable_mapping = std::make_shared<yugawara::analyzer::variable_mapping>();
    variable_mapping->bind(c0, type::int8{});
    variable_mapping->bind(c1, type::int8{});
    variable_mapping->bind(d0, type::int8{});

    yugawara::compiled_info info{expression_mapping, variable_mapping};
    auto pinfo = std::make_shared<processor_info>(rg, info);
    auto p = create_block_variables_definition(pinfo->relations(), pinfo->compiled_info());
    auto& infos = *p.first;
    auto& inds = *p.second;

    ASSERT_EQ(3, infos.size());

    // block 0: c0, c1 defined by take_flat
    EXPECT_TRUE(infos[0].exists_local(c0));
    EXPECT_TRUE(infos[0].exists_local(c1));
    EXPECT_FALSE(infos[0].exists_local(d0));

    // downstream 0 is block 1 (proj + offer0) or block 2 depending on traversal order
    auto blk_proj = inds.at(&proj);  // proj is in the downstream-0 block
    auto blk_r3   = inds.at(&r3);    // r3 is in the downstream-1 block
    EXPECT_NE(blk_proj, blk_r3);

    auto& blk_proj_info = infos[blk_proj];
    auto& blk_r3_info   = infos[blk_r3];

    // downstream-0 block: d0 is local; c0, c1 visible via parent
    EXPECT_TRUE(blk_proj_info.exists_local(d0));
    EXPECT_EQ(blk_proj, blk_proj_info.at(d0).block_index().value());
    EXPECT_TRUE(blk_proj_info.exists(c0));
    EXPECT_EQ(0u, blk_proj_info.at(c0).block_index().value());

    // downstream-1 block: d0 is NOT visible (not local, not in parent chain)
    EXPECT_FALSE(blk_r3_info.exists_local(d0));
    EXPECT_FALSE(blk_r3_info.exists(d0));
    // c0, c1 still visible via parent
    EXPECT_TRUE(blk_r3_info.exists(c0));
    EXPECT_EQ(0u, blk_r3_info.at(c0).block_index().value());
}

}

