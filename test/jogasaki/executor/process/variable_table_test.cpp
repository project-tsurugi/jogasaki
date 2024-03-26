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
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <boost/move/utility_core.hpp>
#include <gtest/gtest.h>

#include <takatori/descriptor/variable.h>
#include <takatori/relation/buffer.h>
#include <takatori/relation/expression_kind.h>
#include <takatori/relation/filter.h>
#include <takatori/relation/graph.h>
#include <takatori/relation/step/offer.h>
#include <takatori/relation/step/take_flat.h>
#include <takatori/scalar/expression_kind.h>
#include <takatori/statement/statement_kind.h>
#include <takatori/type/type_kind.h>
#include <takatori/util/fail.h>
#include <takatori/value/value_kind.h>
#include <yugawara/binding/factory.h>
#include <mizugaki/placeholder_entry.h>
#include <mizugaki/translator/shakujo_translator.h>
#include <mizugaki/translator/shakujo_translator_code.h>
#include <mizugaki/translator/shakujo_translator_options.h>
#include <shakujo/common/core/Type.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/executor/process/impl/variable_table.h>
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

class variable_table_test : public test_root {

};

TEST_F(variable_table_test, basic) {
    factory f;
    auto v1 = f.stream_variable("v1");
    auto v2 = f.exchange_column("v2");
    variable_table_info::variable_indices indices{};
    indices[v1]=0;
    indices[v2]=1;
    std::unordered_map<std::string, takatori::descriptor::variable> names{};
    names.emplace("v1"s, v1);
    names.emplace("v2"s, v2);
    auto rec = mock::create_nullable_record<kind::int4, kind::int4>();
    variable_table_info m{
        std::move(indices),
        std::move(names),
        rec.record_meta()
    };
    variable_table tb{m};
    auto ref = tb.store().ref();
    auto meta = tb.meta();
    ref.set_value(meta->value_offset(0), 1);
    ref.set_null(meta->nullity_offset(0), false);
    ref.set_value(meta->value_offset(1), 10);
    ref.set_null(meta->nullity_offset(1), false);
    std::stringstream ss{};
    ss << tb;
    ASSERT_EQ("v1:1 v2:10", ss.str());
}

TEST_F(variable_table_test, null_value) {
    // verify string representation of null value in the variable table
    factory f;
    auto v1 = f.stream_variable("v1");
    auto v2 = f.exchange_column("v2");
    variable_table_info::variable_indices indices{};
    indices[v1]=0;
    indices[v2]=1;
    std::unordered_map<std::string, takatori::descriptor::variable> names{};
    names.emplace("v1"s, v1);
    names.emplace("v2"s, v2);
    auto rec = mock::create_nullable_record<kind::int4, kind::int4>();
    variable_table_info m{
        std::move(indices),
        std::move(names),
        rec.record_meta()
    };
    variable_table tb{m};
    auto ref = tb.store().ref();
    auto meta = tb.meta();
    ref.set_value(meta->value_offset(0), 1);
    ref.set_null(meta->nullity_offset(0), false);
    ref.set_null(meta->nullity_offset(1), true);
    std::stringstream ss{};
    ss << tb;
    ASSERT_EQ("v1:1 v2:<null>", ss.str());
}

TEST_F(variable_table_test, stringify_non_named_variable_table) {
    // check string representation when variable names are not given
    factory f;
    auto v1 = f.stream_variable("v1");
    auto v2 = f.exchange_column("v2");
    variable_table_info::entity_type map{};
    auto rec = mock::create_nullable_record<kind::int4, kind::int4>();
    auto m = rec.record_meta();
    map[v1] = value_info{m->value_offset(0), m->nullity_offset(0), 0};
    map[v2] = value_info{m->value_offset(1), m->nullity_offset(1), 1};
    variable_table_info info{std::move(map), rec.record_meta()};
    variable_table tb{info};
    auto ref = tb.store().ref();
    auto meta = tb.meta();
    ref.set_value(meta->value_offset(0), 10);
    ref.set_null(meta->nullity_offset(0), false);
    ref.set_value(meta->value_offset(1), 10);
    ref.set_null(meta->nullity_offset(1), false);
    std::stringstream ss{};
    ss << tb;
    ASSERT_EQ("#0:10 #1:10", ss.str()); // variable order can vary
}
}  // namespace jogasaki::executor::process::impl
