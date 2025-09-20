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
#include <cstddef>
#include <decimal.hh>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <gtest/gtest.h>

#include <takatori/decimal/triple.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/executor/expr/details/cast_evaluation.h>
#include <jogasaki/executor/expr/details/decimal_context.h>
#include <jogasaki/executor/expr/error.h>
#include <jogasaki/executor/expr/evaluator.h>
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

using details::handle_ps;

class decimal_handle_ps_test : public test_root {
public:
    void SetUp() override {
        // decimal handling depends on thread local decimal context
        details::ensure_decimal_context();
    }

    memory::page_pool pool_{};
    memory::lifo_paged_memory_resource resource_{&pool_};
};

void check_lost_precision(bool expected, evaluator_context& ctx) {
    EXPECT_EQ(expected, ctx.lost_precision());
    ctx.lost_precision(false);
}

#define lost_precision(arg) {   \
    SCOPED_TRACE("check_lost_precision");   \
    check_lost_precision(arg, ctx); \
}

TEST_F(decimal_handle_ps_test, simple) {
    evaluator_context ctx{&resource_};

    // precision and scale are provided
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 999, 0}}), handle_ps(decimal::Decimal{999}, ctx, 3, 0)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 99, 0}}), handle_ps(decimal::Decimal{99}, ctx, 3, 0)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<triple>, triple{0, 0, 0, 0}}), handle_ps(decimal::Decimal{0}, ctx, 3, 0)); lost_precision(false);

    //   saturated max/min
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 99, -1}}), handle_ps(decimal::Decimal{100}, ctx, 2, 1)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<triple>, triple{-1, 0, 99, -1}}), handle_ps(decimal::Decimal{-100}, ctx, 2, 1)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 99, 0}}), handle_ps(decimal::Decimal{100}, ctx, 2, 0)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 99, -2}}), handle_ps(decimal::Decimal{100}, ctx, 2, 2)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 1001, 0}}), handle_ps(decimal::Decimal{1001}, ctx, 4, 0)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 9999, -1}}), handle_ps(decimal::Decimal{1001}, ctx, 4, 1)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 9999, -2}}), handle_ps(decimal::Decimal{1001}, ctx, 4, 2)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 9999, -3}}), handle_ps(decimal::Decimal{1001}, ctx, 4, 3)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 9999, -4}}), handle_ps(decimal::Decimal{1001}, ctx, 4, 4)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 9999, -4}}), handle_ps(decimal::Decimal{triple{1, 0xFFFFFFFFFFFFFFFFUL, 0xFFFFFFFFFFFFFFFFUL, 0}}, ctx, 4, 4)); lost_precision(true);

    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 12345, -2}}), handle_ps(decimal::Decimal{triple{1, 0, 12345, -2}}, ctx, 5, 2)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 9999, -2}}), handle_ps(decimal::Decimal{triple{1, 0, 12345, -2}}, ctx, 4, 2)); lost_precision(true);

    // only precision, no scale
    EXPECT_EQ((any{std::in_place_type<error>, error_kind::unsupported}), handle_ps(decimal::Decimal{100}, ctx, 2, std::nullopt));

    // only scale is provided
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 1000, -1}}), handle_ps(decimal::Decimal{triple{1, 0, 100, 0}}, ctx, std::nullopt, 1)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 1000, -1}}), handle_ps(decimal::Decimal{triple{1, 0, 10, 1}}, ctx, std::nullopt, 1)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 1000, -1}}), handle_ps(decimal::Decimal{triple{1, 0, 1000, -1}}, ctx, std::nullopt, 1)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 1230, -1}}), handle_ps(decimal::Decimal{123}, ctx, std::nullopt, 1)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 1230, -2}}), handle_ps(decimal::Decimal{triple{1, 0, 123, -1}}, ctx, std::nullopt, 2)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 1234, -2}}), handle_ps(decimal::Decimal{triple{1, 0, 1234, -2}}, ctx, std::nullopt, 2)); lost_precision(false);

    //   truncate fraction part
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 123, -1}}), handle_ps(decimal::Decimal{triple{1, 0, 1234, -2}}, ctx, std::nullopt, 1)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 123, -1}}), handle_ps(decimal::Decimal{triple{1, 0, 12345, -3}}, ctx, std::nullopt, 1)); lost_precision(true);
    EXPECT_EQ((any{std::in_place_type<triple>, static_cast<triple>(decimal::Decimal{"999999999999999999999999999999999999.9"})}), handle_ps(decimal::Decimal{"999999999999999999999999999999999999.99"}, ctx, std::nullopt, 1)); lost_precision(true);
}

TEST_F(decimal_handle_ps_test, same_ps) {
    evaluator_context ctx{&resource_};
    EXPECT_EQ((any{std::in_place_type<triple>, triple{0, 0, 0, -3}}), handle_ps(decimal::Decimal{0}, ctx, 3, 3)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<triple>, triple{0, 0, 0, -3}}), handle_ps(decimal::Decimal{"0.000"}, ctx, 3, 3)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 999, -3}}), handle_ps(decimal::Decimal{"0.999"}, ctx, 3, 3)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 990, -3}}), handle_ps(decimal::Decimal{"0.990"}, ctx, 3, 3)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 900, -3}}), handle_ps(decimal::Decimal{"0.900"}, ctx, 3, 3)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 1, -3}}), handle_ps(decimal::Decimal{"0.001"}, ctx, 3, 3)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 10, -3}}), handle_ps(decimal::Decimal{"0.010"}, ctx, 3, 3)); lost_precision(false);
    EXPECT_EQ((any{std::in_place_type<triple>, triple{1, 0, 100, -3}}), handle_ps(decimal::Decimal{"0.100"}, ctx, 3, 3)); lost_precision(false);
}

}

