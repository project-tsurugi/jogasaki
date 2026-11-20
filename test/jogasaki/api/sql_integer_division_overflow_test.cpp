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

#include <memory>
#include <string>
#include <vector>
#include <gtest/gtest.h>

#include <jogasaki/configuration.h>
#include <jogasaki/error_code.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/mock/basic_record.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using kind = meta::field_type_kind;

// regression test for issue #1620 - dividing the minimum value of INT/BIGINT by -1 crashed the server
class sql_integer_division_overflow_test :
    public ::testing::Test,
    public api_test_base {

public:
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        db_setup(cfg);
    }

    void TearDown() override {
        db_teardown();
    }
};

TEST_F(sql_integer_division_overflow_test, int_min_by_minus_one) {
    test_stmt_err("select cast(-2147483648 as int) / cast(-1 as int)", error_code::value_evaluation_exception);
    {
        std::vector<mock::basic_record> result{};
        execute_query("select cast(-2147483648 as int) % cast(-1 as int)", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int4>(0)), result[0]);
    }
    {
        // other values divided by -1 are not affected
        std::vector<mock::basic_record> result{};
        execute_query("select cast(-2147483647 as int) / cast(-1 as int)", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int4>(2147483647)), result[0]);
    }
}

TEST_F(sql_integer_division_overflow_test, bigint_min_by_minus_one) {
    test_stmt_err(
        "select cast(-9223372036854775808 as bigint) / cast(-1 as bigint)",
        error_code::value_evaluation_exception
    );
    {
        std::vector<mock::basic_record> result{};
        execute_query("select cast(-9223372036854775808 as bigint) % cast(-1 as bigint)", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int8>(0)), result[0]);
    }
    {
        // other values divided by -1 are not affected
        std::vector<mock::basic_record> result{};
        execute_query("select cast(-9223372036854775807 as bigint) / cast(-1 as bigint)", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int8>(9223372036854775807)), result[0]);
    }
}

}  // namespace jogasaki::testing
