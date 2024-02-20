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
#include <jogasaki/executor/process/impl/variable_table.h>

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

#include <takatori/descriptor/variable.h>
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

    auto& i1 = m.at(v1);
    ASSERT_EQ(0, i1.value_offset());
    auto& i2 = m.at(v2);
    ASSERT_EQ(32, i2.value_offset());
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

    auto& i1 = m.at(v1);
    ASSERT_EQ(0, i1.value_offset());
    auto& i2 = m.at(v2);
    ASSERT_EQ(32, i2.value_offset());
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

}  // namespace jogasaki::executor::process::impl
