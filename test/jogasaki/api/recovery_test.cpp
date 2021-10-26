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
#include <jogasaki/api.h>

#include <thread>
#include <gtest/gtest.h>
#include <glog/logging.h>

#include <jogasaki/test_utils.h>
#include <jogasaki/accessor/record_printer.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/executor/sequence/manager.h>
#include <jogasaki/executor/sequence/sequence.h>
#include <jogasaki/api/field_type_kind.h>
#include <jogasaki/scheduler/task_scheduler.h>
#include "api_test_base.h"
#include <jogasaki/kvs/id.h>

namespace jogasaki::testing {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace std::chrono_literals;

using namespace yugawara;
using namespace yugawara::storage;

/**
 * @brief test database recovery
 */
class recovery_test :
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
        db_setup(cfg);

        auto* impl = db_impl();
        add_benchmark_tables(*impl->tables());
        register_kvs_storage(*impl->kvs_db(), *impl->tables());
    }

    void TearDown() override {
        db_teardown();
    }
};

TEST_F(recovery_test, DISABLED_simple) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory doesn't support recovery";
    }
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (2, 20)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (3, 30)");
    wait_epochs(10);
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T0", result);
        ASSERT_EQ(3, result.size());
    }
    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T0", result);
        ASSERT_EQ(3, result.size());
    }
}

TEST_F(recovery_test, DISABLED_system_table) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory doesn't support recovery";
    }
    std::size_t sequences{};
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM "s+std::string{system_sequences_name}, result);
        sequences = result.size();
        LOG(INFO) << "sequences: " << sequences;
    }
    jogasaki::executor::sequence::manager mgr{*db_impl()->kvs_db()};
    mgr.register_sequence(100, "SEQ100");
    mgr.register_sequence(200, "SEQ200");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM "s+std::string{system_sequences_name}, result);
        ASSERT_EQ(sequences+2, result.size());
    }

    EXPECT_EQ(0, mgr.load_id_map());
    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM "s+std::string{system_sequences_name}, result);
        ASSERT_EQ(sequences+2, result.size());
    }
}

TEST_F(recovery_test, DISABLED_delete) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory doesn't support recovery";
    }
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (2, 20)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (3, 30)");
    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T0", result);
        ASSERT_EQ(3, result.size());
    }
    execute_statement( "DELETE FROM T0 WHERE C0=2");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T0", result);
        ASSERT_EQ(2, result.size());
    }
    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T0", result);
        ASSERT_EQ(2, result.size());
    }
}
}
