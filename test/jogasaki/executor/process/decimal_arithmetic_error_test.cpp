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
#include <decimal.hh>
#include <iostream>
#include <string>
#include <string_view>
#include <gtest/gtest.h>

#include <takatori/decimal/triple.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/executor/expr/details/cast_evaluation.h>
#include <jogasaki/executor/expr/details/decimal_context.h>
#include <jogasaki/executor/expr/evaluator_context.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/memory/page_pool.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/test_root.h>
#include <jogasaki/test_utils.h>

namespace jogasaki::executor::expr {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace meta;

using namespace testing;

using accessor::text;
using takatori::decimal::triple;

class decimal_arithmetic_error_test : public test_root {
public:
    memory::page_pool pool_{};
    memory::lifo_paged_memory_resource resource_{&pool_};
};

TEST_F(decimal_arithmetic_error_test, overflow_add) {
    decimal::context = details::standard_decimal_context();
    evaluator_context ctx{&resource_};
    auto a = add_any(
        any{std::in_place_type<triple>, triple{1, 0, 5, 24576}},
        any{std::in_place_type<triple>, triple{1, 0, 5, 24576}},
        ctx
    );
    EXPECT_FALSE(a) << ctx;
    EXPECT_EQ(error_kind::arithmetic_error, a.to<error>().kind());
    std::cerr << ctx << std::endl;
}

TEST_F(decimal_arithmetic_error_test, overflow_sub) {
    decimal::context = details::standard_decimal_context();
    evaluator_context ctx{&resource_};
    auto a = subtract_any(
        any{std::in_place_type<triple>, triple{1, 0, 5, 24576}},
        any{std::in_place_type<triple>, triple{-1, 0, 5, 24576}},
        ctx
    );
    EXPECT_FALSE(a);
    EXPECT_EQ(error_kind::arithmetic_error, a.to<error>().kind());
    std::cerr << ctx << std::endl;
}

TEST_F(decimal_arithmetic_error_test, overflow_mult) {
    decimal::context = details::standard_decimal_context();
    evaluator_context ctx{&resource_};
    auto a = multiply_any(
        any{std::in_place_type<triple>, triple{1, 0, 1, 24576}},
        any{std::in_place_type<triple>, triple{1, 0, 10, 0}},
        ctx
    );
    EXPECT_FALSE(a);
    EXPECT_EQ(error_kind::arithmetic_error, a.to<error>().kind());
    std::cerr << ctx << std::endl;
}

TEST_F(decimal_arithmetic_error_test, overflow_div) {
    decimal::context = details::standard_decimal_context();
    evaluator_context ctx{&resource_};
    auto a = divide_any(
        any{std::in_place_type<triple>, triple{1, 0, 1, 24576}},
        any{std::in_place_type<triple>, triple{1, 0, 1, -1}},
        ctx
    );
    EXPECT_FALSE(a);
    EXPECT_EQ(error_kind::arithmetic_error, a.to<error>().kind());
    std::cerr << ctx << std::endl;
}

TEST_F(decimal_arithmetic_error_test, overflow_rem) {
    decimal::context = details::standard_decimal_context();
    evaluator_context ctx{&resource_};
    auto a = remainder_any(
        any{std::in_place_type<triple>, triple{1, 0, 1, 24576}},
        any{std::in_place_type<triple>, triple{1, 0, 1, -1}},
        ctx
    );
    EXPECT_FALSE(a);
    EXPECT_EQ(error_kind::arithmetic_error, a.to<error>().kind());
    std::cerr << ctx << std::endl;
}

TEST_F(decimal_arithmetic_error_test, zero_division_div) {
    decimal::context = details::standard_decimal_context();
    evaluator_context ctx{&resource_};
    auto a = divide_any(
        any{std::in_place_type<triple>, triple{1, 0, 1, 0}},
        any{std::in_place_type<triple>, triple{1, 0, 0, 0}},
        ctx
    );
    EXPECT_FALSE(a);
    EXPECT_EQ(error_kind::arithmetic_error, a.to<error>().kind());
    std::cerr << ctx << std::endl;
}

TEST_F(decimal_arithmetic_error_test, zero_division_rem) {
    decimal::context = details::standard_decimal_context();
    evaluator_context ctx{&resource_};
    auto a = remainder_any(
        any{std::in_place_type<triple>, triple{1, 0, 1, 0}},
        any{std::in_place_type<triple>, triple{1, 0, 0, 0}},
        ctx
    );
    EXPECT_FALSE(a);
    EXPECT_EQ(error_kind::arithmetic_error, a.to<error>().kind());
    std::cerr << ctx << std::endl;
}

}

