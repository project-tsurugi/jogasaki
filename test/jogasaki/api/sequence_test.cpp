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

#include <regex>
#include <gtest/gtest.h>

#include <takatori/util/downcast.h>

#include <jogasaki/executor/common/graph.h>
#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/data/any.h>

#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/kvs/id.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/utils/storage_data.h>
#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/impl/record.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/executor/tables.h>
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
        cfg->single_thread(true);
        cfg->prepare_test_tables(true);
        db_setup(cfg);
    }

    void TearDown() override {
        db_teardown();
    }
};

using namespace std::string_view_literals;

TEST_F(sequence_test, generate_primary_key) {
    {
        std::vector<mock::basic_record> entries{};
        execute_query("SELECT * FROM __system_sequences", entries);
        ASSERT_LT(0, entries.size());
    }
    execute_statement( "INSERT INTO TSEQ0 (C1) VALUES (10)");
    execute_statement( "INSERT INTO TSEQ0 (C1) VALUES (20)");
    execute_statement( "INSERT INTO TSEQ0 (C1) VALUES (30)");

    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM TSEQ0 ORDER BY C1", result);
    ASSERT_EQ(3, result.size());
    auto meta = result[0].record_meta();
    auto s0 = result[0].ref().get_value<std::int64_t>(meta->value_offset(0));
    auto s1 = result[1].ref().get_value<std::int64_t>(meta->value_offset(0));
    auto s2 = result[2].ref().get_value<std::int64_t>(meta->value_offset(0));
    EXPECT_LT(s0, s1);
    EXPECT_LT(s1, s2);
    {
        std::vector<mock::basic_record> entries{};
        execute_query("SELECT * FROM __system_sequences", entries);
        ASSERT_LT(0, entries.size());
    }
}

TEST_F(sequence_test, recovery) {
    utils::set_global_tx_option({false, false}); // to customize
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory doesn't support recovery";
    }
    execute_statement( "INSERT INTO TSEQ0 (C1) VALUES (10)");
    execute_statement( "INSERT INTO TSEQ0 (C1) VALUES (20)");
    {
        SCOPED_TRACE("initial");
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM TSEQ0", result);
        ASSERT_EQ(2, result.size());
    }
    int num_seqs = 0;
    {
        SCOPED_TRACE("before recovery 0");
        std::vector<mock::basic_record> entries{};
        execute_query("SELECT * FROM __system_sequences", entries);
        num_seqs = entries.size();
        ASSERT_LT(0, num_seqs);
    }
    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());
    {
        SCOPED_TRACE("after recovery 0");
        std::vector<mock::basic_record> entries{};
        execute_query("SELECT * FROM __system_sequences", entries);
        ASSERT_EQ(num_seqs, entries.size());
    }
    execute_statement( "INSERT INTO TSEQ0 (C1) VALUES (30)");
    {
        SCOPED_TRACE("before recovery 1");
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM TSEQ0 ORDER BY C1", result);
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
    {
        SCOPED_TRACE("after recovery 0");
        std::vector<mock::basic_record> entries{};
        execute_query("SELECT * FROM __system_sequences", entries);
        ASSERT_EQ(num_seqs, entries.size());
    }
    execute_statement( "INSERT INTO TSEQ0 (C1) VALUES (40)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM TSEQ0 ORDER BY C1", result);
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
