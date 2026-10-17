/*
 * Copyright 2018-2026 Project Tsurugi.
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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>
#include <decimal.hh>
#include <gtest/gtest.h>

#include <takatori/decimal/triple.h>

#include <jogasaki/configuration.h>
#include <jogasaki/mock/basic_record.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using triple = takatori::decimal::triple;

/**
 * @brief tests for decimal values that are numerically equal but have different representations
 * @details numerically equal decimals can have different triple representations (e.g. 1.0 = (10, -1) and 1 = (1, 0)).
 * Such values are created by expressions, e.g. conditional expression whose branches are converted differently
 * to the result type decimal(*,*) (the converted branch is reduced, while the other is kept as it is).
 * They must be treated as the same value in grouping, distinct and join.
 */
class sql_decimal_representation_test :
    public ::testing::Test,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        db_setup(cfg);

        // `case when pk < 10 then c0 * 1 else c1 end` evaluates to 1 with different representations
        // - (10, -1) for the rows pk < 10 and (1, 0) for others
        execute_statement("create table t (pk int primary key, c0 decimal(5,1), c1 decimal(5,2))");
        for(std::size_t i = 0; i < 20; ++i) {
            execute_statement("insert into t values (" + std::to_string(i) + ", 1.0, 1.00)");
        }
    }

    void TearDown() override {
        db_teardown();
    }

    std::vector<mock::basic_record> query(std::string_view sql) {
        std::vector<mock::basic_record> result{};
        execute_query(sql, result);
        std::sort(result.begin(), result.end());
        return result;
    }
};

TEST_F(sql_decimal_representation_test, group_by) {
    auto result = query(
        "SELECT k, count(*) FROM (SELECT case when pk < 10 then c0 * 1 else c1 end AS k FROM t) r GROUP BY k"
    );
    ASSERT_EQ(1, result.size());
    EXPECT_EQ(0, static_cast<decimal::Decimal>(result[0].get_value<triple>(0)).compare(decimal::Decimal{1}));
    EXPECT_EQ(20, result[0].get_value<std::int64_t>(1));
}

TEST_F(sql_decimal_representation_test, group_by_with_count_distinct) {
    // count distinct is processed by group exchange and aggregate_group operator, not by aggregate exchange
    auto result = query(
        "SELECT k, count(distinct pk) FROM (SELECT pk, case when pk < 10 then c0 * 1 else c1 end AS k FROM t) r GROUP BY k"
    );
    ASSERT_EQ(1, result.size());
    EXPECT_EQ(0, static_cast<decimal::Decimal>(result[0].get_value<triple>(0)).compare(decimal::Decimal{1}));
    EXPECT_EQ(20, result[0].get_value<std::int64_t>(1));
}

TEST_F(sql_decimal_representation_test, select_distinct) {
    auto result = query("SELECT DISTINCT case when pk < 10 then c0 * 1 else c1 end FROM t");
    ASSERT_EQ(1, result.size());
    EXPECT_EQ(0, static_cast<decimal::Decimal>(result[0].get_value<triple>(0)).compare(decimal::Decimal{1}));
}

TEST_F(sql_decimal_representation_test, count_distinct) {
    auto result = query("SELECT count(distinct case when pk < 10 then c0 * 1 else c1 end) FROM t");
    ASSERT_EQ(1, result.size());
    EXPECT_EQ(1, result[0].get_value<std::int64_t>(0));
}

TEST_F(sql_decimal_representation_test, join) {
    // join keys are 1.0 on the left and 1 on the right
    auto result = query(
        "SELECT count(*) FROM "
        "(SELECT pk, c0 * 1 AS k FROM t WHERE pk < 10) a JOIN "
        "(SELECT pk, case when pk < 0 then c0 * 1 else c1 end AS k FROM t WHERE pk >= 10) b ON a.k = b.k"
    );
    ASSERT_EQ(1, result.size());
    EXPECT_EQ(100, result[0].get_value<std::int64_t>(0));
}

}  // namespace jogasaki::testing
