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
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <gtest/gtest.h>

#include <takatori/decimal/triple.h>
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
#include <takatori/type/type_kind.h>
#include <takatori/util/exception.h>
#include <takatori/value/value_kind.h>
#include <yugawara/binding/factory.h>
#include <yugawara/compiled_info.h>

#include <jogasaki/data/any.h>
#include <jogasaki/executor/process/impl/expression/details/cast_evaluation.h>
#include <jogasaki/executor/process/impl/expression/details/common.h>
#include <jogasaki/executor/process/impl/expression/details/decimal_context.h>
#include <jogasaki/executor/process/impl/expression/error.h>
#include <jogasaki/executor/process/impl/expression/evaluator.h>
#include <jogasaki/executor/process/impl/expression/evaluator_context.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/memory/page_pool.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/test_root.h>
#include <jogasaki/test_utils.h>
#include <jogasaki/test_utils/make_triple.h>

namespace jogasaki::executor::process::impl::expression {

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

using rgraph = ::takatori::relation::graph_type;

using binary = takatori::scalar::binary;
using binary_operator = takatori::scalar::binary_operator;
using compare = takatori::scalar::compare;
using comparison_operator = takatori::scalar::comparison_operator;
using unary = takatori::scalar::unary;
using unary_operator = takatori::scalar::unary_operator;
using immediate = takatori::scalar::immediate;
using compiled_info = yugawara::compiled_info;

using takatori::decimal::triple;

class cast_octet_test : public test_root {
public:
    void SetUp() override {
        // decimal handling depends on thread local decimal context
        details::ensure_decimal_context();
    }

    memory::page_pool pool_{};
    memory::lifo_paged_memory_resource resource_{&pool_};
};

using namespace details::from_octet;

void check_lost_precision(bool expected, evaluator_context& ctx) {
    EXPECT_EQ(expected, ctx.lost_precision());
    ctx.lost_precision(false);
}

#define lost_precision(arg) {   \
    SCOPED_TRACE("check_lost_precision");   \
    check_lost_precision(arg, ctx); \
}

any any_binary(std::string_view s) {
    return any{std::in_place_type<accessor::binary>, s};
}

TEST_F(cast_octet_test, from_octet) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ(any_binary("\x01"sv), details::from_octet::to_octet("\x01"sv, ctx, std::nullopt, false, false)); lost_precision(false);
    EXPECT_EQ(any_binary("\x01\x00\x00"sv), details::from_octet::to_octet("\x01"sv, ctx, 3, true, false)); lost_precision(false);
    EXPECT_EQ(any_binary("\x01\x00"sv), details::from_octet::to_octet("\x01\x00\x00"sv, ctx, 2, false, false)); lost_precision(true);
    EXPECT_EQ(any_binary("\x01\x00"sv), details::from_octet::to_octet("\x01\x00\x00"sv, ctx, 2, false, true)); lost_precision(false);
    EXPECT_EQ(any_binary("\x01\x00"sv), details::from_octet::to_octet("\x01\x00\x02"sv, ctx, 2, true, true)); lost_precision(true);
}

}

