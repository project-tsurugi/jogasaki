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
#include <takatori/relation/step/offer.h>
#include <takatori/relation/step/take_flat.h>
#include <takatori/scalar/expression_kind.h>
#include <takatori/scalar/immediate.h>
#include <takatori/statement/statement_kind.h>
#include <takatori/tree/tree_element_vector.h>
#include <takatori/type/primitive.h>
#include <takatori/type/type_kind.h>
#include <takatori/util/fail.h>
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

}

