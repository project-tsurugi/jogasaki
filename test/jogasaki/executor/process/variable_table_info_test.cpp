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
#include <jogasaki/executor/process/impl/variable_table_info.h>

#include <gtest/gtest.h>
#include <glog/logging.h>


#include <shakujo/parser/Parser.h>
#include <shakujo/common/core/Type.h>
#include <shakujo/model/IRFactory.h>

#include <yugawara/storage/configurable_provider.h>
#include <yugawara/binding/factory.h>
#include <yugawara/runtime_feature.h>
#include <yugawara/compiler.h>
#include <yugawara/compiler_options.h>

#include <mizugaki/translator/shakujo_translator.h>

#include <takatori/type/int.h>
#include <takatori/type/float.h>
#include <takatori/value/int.h>
#include <takatori/value/float.h>
#include <takatori/value/boolean.h>
#include <takatori/util/string_builder.h>
#include <takatori/util/downcast.h>
#include <takatori/relation/emit.h>
#include <takatori/relation/step/take_flat.h>
#include <takatori/relation/step/offer.h>
#include <takatori/relation/buffer.h>
#include <takatori/relation/scan.h>
#include <takatori/relation/filter.h>
#include <takatori/relation/project.h>
#include <takatori/statement/write.h>
#include <takatori/statement/execute.h>
#include <takatori/scalar/immediate.h>
#include <takatori/plan/process.h>
#include <takatori/plan/forward.h>
#include <takatori/serializer/json_printer.h>

#include <takatori/util/enum_tag.h>
#include <jogasaki/utils/field_types.h>
#include <jogasaki/test_utils.h>
#include <jogasaki/test_root.h>

#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/executor/process/impl/ops/operator_builder.h>

namespace jogasaki::executor::process::impl {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace meta;
using namespace takatori::util;
using namespace yugawara::binding;

using namespace ::mizugaki::translator;
using namespace ::mizugaki;

using namespace testing;

using code = shakujo_translator_code;
using result_kind = shakujo_translator::result_type::kind_type;

namespace type = ::takatori::type;
namespace value = ::takatori::value;
namespace scalar = ::takatori::scalar;
namespace relation = ::takatori::relation;
namespace statement = ::takatori::statement;

namespace tinfo = ::shakujo::common::core::type;

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
    map[v1] = value_info{1, 1};
    map[v2] = value_info{2,2};

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
    map[v1] = value_info{1, 1};
    map[v2] = value_info{2,2};

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
    auto [infos, inds] = create_block_variables_definition(pinfo->relations(), pinfo->compiled_info());

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

