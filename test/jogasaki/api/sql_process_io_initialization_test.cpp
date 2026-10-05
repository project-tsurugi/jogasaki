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
#include <memory>

#include <gtest/gtest.h>

#include <jogasaki/api/database.h>
#include <jogasaki/api/executable_statement.h>
#include <jogasaki/api/record.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/result_set_iterator.h>
#include <jogasaki/utils/create_tx.h>

#include "api_test_base.h"

namespace jogasaki::testing {

class sql_process_io_initialization_test : public ::testing::Test, public api_test_base {
    bool to_explain() override { return false; }
    void SetUp() override { db_setup(); }
    void TearDown() override { db_teardown(); }
};

TEST_F(sql_process_io_initialization_test, reuses_executable_with_exchange_metadata) {
    execute_statement("create table t (k int primary key, v bigint)");
    execute_statement("insert into t values (1, 10), (2, 20), (3, 30)");
    std::unique_ptr<api::executable_statement> statement{};
    ASSERT_EQ(status::ok, db_->create_executable("select sum(v) from t", statement));
    ASSERT_TRUE(statement);
    for (int i = 0; i < 3; ++i) {
        auto tx = utils::create_transaction(*db_);
        std::unique_ptr<api::result_set> results{};
        ASSERT_EQ(status::ok, tx->execute(*statement, results));
        auto iterator = results->iterator();
        ASSERT_TRUE(iterator->has_next());
        auto* record = iterator->next();
        EXPECT_FALSE(record->is_null(0));
        EXPECT_EQ(60, record->get_int8(0));
        EXPECT_FALSE(iterator->has_next());
        results.reset();
        ASSERT_EQ(status::ok, tx->commit());
    }
}

} // namespace jogasaki::testing
