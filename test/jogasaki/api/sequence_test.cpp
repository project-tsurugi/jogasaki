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

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>
#include <gtest/gtest.h>

#include <takatori/util/downcast.h>
#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/configuration.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/kvs/id.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/port.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/status.h>
#include <jogasaki/utils/create_tx.h>
#include <jogasaki/utils/tables.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;

using takatori::util::unsafe_downcast;

class sequence_test :
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
    }

    void TearDown() override {
        db_teardown();
    }
};

using namespace std::string_view_literals;

TEST_F(sequence_test, generate_primary_key) {
    execute_statement("create table t (C1 INT)");
    execute_statement("INSERT INTO t (C1) VALUES (10)");
    execute_statement("INSERT INTO t (C1) VALUES (10)");
    execute_statement("INSERT INTO t (C1) VALUES (10)");

    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM t ORDER BY C1", result);
    ASSERT_EQ(3, result.size());
}

TEST_F(sequence_test, recovery) {
    utils::set_global_tx_option({false, false}); // to customize
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory doesn't support recovery";
    }
    execute_statement("CREATE TABLE T (C0 BIGINT GENERATED ALWAYS AS IDENTITY, C1 BIGINT)");
    execute_statement("INSERT INTO T (C1) VALUES (10)");
    execute_statement("INSERT INTO T (C1) VALUES (20)");
    {
        SCOPED_TRACE("initial");
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T", result);
        ASSERT_EQ(2, result.size());
    }
    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());
    execute_statement( "INSERT INTO T (C1) VALUES (30)");
    {
        SCOPED_TRACE("before recovery 1");
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T ORDER BY C1", result);
        ASSERT_EQ(3, result.size());
        auto meta = result[0].record_meta();
        auto s0 = result[0].ref().get_value<std::int64_t>(meta->value_offset(0));
        auto s1 = result[1].ref().get_value<std::int64_t>(meta->value_offset(0));
        auto s2 = result[2].ref().get_value<std::int64_t>(meta->value_offset(0));
        EXPECT_LT(s0, s1);
        EXPECT_LT(s1, s2);
    }
    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());
    execute_statement( "INSERT INTO T (C1) VALUES (40)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T ORDER BY C1", result);
        ASSERT_EQ(4, result.size());
        auto meta = result[0].record_meta();
        auto s0 = result[0].ref().get_value<std::int64_t>(meta->value_offset(0));
        auto s1 = result[1].ref().get_value<std::int64_t>(meta->value_offset(0));
        auto s2 = result[2].ref().get_value<std::int64_t>(meta->value_offset(0));
        auto s3 = result[3].ref().get_value<std::int64_t>(meta->value_offset(0));
        EXPECT_LT(s0, s1);
        EXPECT_LT(s1, s2);
        EXPECT_LT(s2, s3);
    }
}

}
