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
#include <jogasaki/data/value.h>

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <boost/dynamic_bitset.hpp>

#include <jogasaki/executor/partitioner.h>
#include <jogasaki/executor/comparator.h>

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
#include <takatori/scalar/unary.h>
#include <takatori/scalar/unary_operator.h>
#include <takatori/plan/process.h>
#include <takatori/plan/forward.h>
#include <takatori/serializer/json_printer.h>
#include <takatori/scalar/binary.h>
#include <takatori/scalar/binary_operator.h>
#include <takatori/scalar/compare.h>
#include <takatori/scalar/comparison_operator.h>

#include <jogasaki/utils/field_types.h>
#include <jogasaki/test_utils.h>
#include <jogasaki/test_root.h>

#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/executor/process/impl/ops/operator_builder.h>
#include <jogasaki/executor/process/impl/variable_table.h>

namespace jogasaki::executor::process::impl::expression {

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


using take = relation::step::take_flat;
using offer = relation::step::offer;
using buffer = relation::buffer;

using rgraph = ::takatori::relation::graph_type;

class value_test : public test_root {
public:
};

using binary = takatori::scalar::binary;
using binary_operator = takatori::scalar::binary_operator;
using compare = takatori::scalar::compare;
using comparison_operator = takatori::scalar::comparison_operator;
using unary = takatori::scalar::unary;
using unary_operator = takatori::scalar::unary_operator;
using immediate = takatori::scalar::immediate;
using compiled_info = yugawara::compiled_info;

TEST_F(value_test, simple) {
    data::value v{};
    ASSERT_FALSE(v);
    ASSERT_TRUE(v.empty());
    v = data::value{std::in_place_type<std::int32_t>, 1};
    ASSERT_TRUE(v);
    ASSERT_FALSE(v.empty());
    ASSERT_EQ(1, v.ref<std::int32_t>());
}

TEST_F(value_test, fail_on_type_mismatch) {
    data::value v{};
    ASSERT_DEATH({ (void)v.ref<std::int32_t>(); }, "fail");

    v = data::value{std::in_place_type<std::int64_t>, 1};
    ASSERT_DEATH({ (void)v.ref<std::int32_t>(); }, "fail");
}

TEST_F(value_test, bool) {
    // bool and std::int8_t can be used synonymously
    {
        auto v = data::value{std::in_place_type<std::int8_t>, 1};
        ASSERT_TRUE(v);
        ASSERT_FALSE(v.empty());
        ASSERT_EQ(1, v.ref<std::int8_t>());
        ASSERT_TRUE(v.ref<bool>());
    }
    {
        auto v = data::value{std::in_place_type<std::int8_t>, 0};
        ASSERT_TRUE(v);
        ASSERT_FALSE(v.empty());
        ASSERT_EQ(0, v.ref<std::int8_t>());
        ASSERT_FALSE(v.ref<bool>());
    }
    {
        auto v = data::value{std::in_place_type<bool>, true};
        ASSERT_TRUE(v);
        ASSERT_FALSE(v.empty());
        ASSERT_EQ(1, v.ref<std::int8_t>());
        ASSERT_TRUE(v.ref<bool>());
    }
    {
        auto v = data::value{std::in_place_type<bool>, false};
        ASSERT_TRUE(v);
        ASSERT_FALSE(v.empty());
        ASSERT_EQ(0, v.ref<std::int8_t>());
        ASSERT_FALSE(v.ref<bool>());
    }
}
}
