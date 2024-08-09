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
#include <jogasaki/executor/process/impl/expression/single_function_evaluator.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <decimal.hh>
#include <float.h>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>
#include <boost/cstdint.hpp>
#include <boost/dynamic_bitset/dynamic_bitset.hpp>
#include <gtest/gtest.h>

#include <takatori/datetime/date.h>
#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/time_point.h>
#include <takatori/decimal/triple.h>
#include <takatori/descriptor/element.h>
#include <takatori/descriptor/variable.h>
#include <takatori/relation/buffer.h>
#include <takatori/relation/expression_kind.h>
#include <takatori/relation/graph.h>
#include <takatori/relation/step/offer.h>
#include <takatori/relation/step/take_flat.h>
#include <takatori/scalar/binary.h>
#include <takatori/scalar/binary_operator.h>
#include <takatori/scalar/compare.h>
#include <takatori/scalar/comparison_operator.h>
#include <takatori/scalar/expression_kind.h>
#include <takatori/scalar/immediate.h>
#include <takatori/scalar/unary.h>
#include <takatori/scalar/unary_operator.h>
#include <takatori/statement/statement_kind.h>
#include <takatori/type/character.h>
#include <takatori/type/data.h>
#include <takatori/type/date.h>
#include <takatori/type/decimal.h>
#include <takatori/type/primitive.h>
#include <takatori/type/time_of_day.h>
#include <takatori/type/time_point.h>
#include <takatori/type/type_kind.h>
#include <takatori/type/varying.h>
#include <takatori/util/exception.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/value/primitive.h>
#include <takatori/value/value_kind.h>
#include <yugawara/analyzer/expression_mapping.h>
#include <yugawara/analyzer/variable_mapping.h>
#include <yugawara/binding/factory.h>
#include <yugawara/compiled_info.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/accessor/text.h>
#include <jogasaki/data/any.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/executor/function/builtin_scalar_functions.h>
#include <jogasaki/executor/function/function_evaluation_context.h>
#include <jogasaki/executor/function/scalar_function_repository.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/process/impl/expression/error.h>
#include <jogasaki/executor/process/impl/expression/evaluator.h>
#include <jogasaki/executor/process/impl/expression/evaluator_context.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/memory/page_pool.h>
#include <jogasaki/meta/character_field_option.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_traits.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/test_root.h>
#include <jogasaki/test_utils.h>
#include <jogasaki/test_utils/to_field_type_kind.h>
#include <jogasaki/utils/checkpoint_holder.h>
#include <jogasaki/utils/field_types.h>

namespace jogasaki::executor::process::impl::expression {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace std::chrono_literals;
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

using rgraph = ::takatori::relation::graph_type;

using binary = takatori::scalar::binary;
using binary_operator = takatori::scalar::binary_operator;
using compare = takatori::scalar::compare;
using comparison_operator = takatori::scalar::comparison_operator;
using unary = takatori::scalar::unary;
using unary_operator = takatori::scalar::unary_operator;
using immediate = takatori::scalar::immediate;
using compiled_info = yugawara::compiled_info;
using variable = takatori::descriptor::variable;

using takatori::datetime::date;
using takatori::datetime::time_of_day;
using takatori::datetime::time_point;

class single_function_evaluator_test : public test_root {
public:

    memory::page_pool pool_{};
    memory::lifo_paged_memory_resource resource_{&pool_};

};

TEST_F(single_function_evaluator_test, current_timestamp) {
    auto& rep = global::scalar_function_repository();
    ::yugawara::function::configurable_provider functions{};
    executor::function::add_builtin_scalar_functions(functions, rep);
    single_function_evaluator eval{11004, functions};

    utils::checkpoint_holder cph{&resource_};
    using clock = std::chrono::system_clock;
    auto fctx = std::make_shared<function::function_evaluation_context>();
    takatori::datetime::time_point tp{date{2021, 1, 1}, time_of_day{0,0,0}};
    fctx->transaction_begin(clock::time_point{tp.seconds_since_epoch()});
    evaluator_context c{&resource_, std::move(fctx)};
    auto a = eval(c);
    ASSERT_TRUE(a);
    ASSERT_EQ(tp, a.to<takatori::datetime::time_point>());
}

}  // namespace jogasaki::executor::process::impl::expression
