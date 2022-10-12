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

#include <takatori/type/int.h>
#include <yugawara/variable/nullity.h>

#include <jogasaki/test_utils.h>
#include <jogasaki/accessor/record_printer.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/executor/sequence/manager.h>
#include <jogasaki/executor/sequence/sequence.h>
#include <jogasaki/api/field_type_kind.h>
#include <jogasaki/scheduler/task_scheduler.h>
#include "api_test_base.h"
#include <jogasaki/kvs/id.h>
#include <jogasaki/utils/storage_dump_formatter.h>

namespace jogasaki::testing {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace std::chrono_literals;

using namespace yugawara;
using namespace yugawara::storage;

namespace type = takatori::type;
using nullity = yugawara::variable::nullity;
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
        impl->initialize_from_providers();
    }

    void TearDown() override {
        db_teardown();
    }

    void dump_content() {
        utils::storage_dump_formatter f{};
        auto out = std::cerr << f;
        db_->dump(out, "T0", 100);
    }
};

TEST_F(recovery_test, simple) {
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
    dump_content();
}

TEST_F(recovery_test, recover_twice) {
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
    dump_content();
    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());
    wait_epochs(10);
    dump_content();
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T0", result);
        ASSERT_EQ(3, result.size());
    }
    wait_epochs(10);
    dump_content();
    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());
    wait_epochs(10);
    dump_content();
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T0", result);
        ASSERT_EQ(3, result.size());
    }
}

TEST_F(recovery_test, system_table) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory doesn't support recovery";
    }
    std::size_t sequences{};
    {
        SCOPED_TRACE("initial");
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM "s+std::string{system_sequences_name}, result);
        sequences = result.size();
        LOG(INFO) << "built-in sequences: " << sequences;
    }
    jogasaki::executor::sequence::manager mgr{*db_impl()->kvs_db()};
    mgr.register_sequence(nullptr, 100, "SEQ100");
    mgr.register_sequence(nullptr, 200, "SEQ200");
    {
        SCOPED_TRACE("sequences are registered");
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM "s+std::string{system_sequences_name}, result);
        ASSERT_EQ(sequences+2, result.size());
    }

    EXPECT_EQ(sequences+2, mgr.load_id_map());
    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());
    {
        SCOPED_TRACE("after recovery");
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM "s+std::string{system_sequences_name}, result);
        ASSERT_EQ(sequences+2, result.size());
    }
}

TEST_F(recovery_test, recovery_metadata) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory doesn't support recovery";
    }
    execute_statement("CREATE TABLE RECOVER (C0 INT PRIMARY KEY, C1 INT)");
    execute_statement("INSERT INTO RECOVER (C0, C1) VALUES (1, 10)");
    execute_statement("INSERT INTO RECOVER (C0, C1) VALUES (2, 20)");
    execute_statement("INSERT INTO RECOVER (C0, C1) VALUES (3, 30)");
    {
        SCOPED_TRACE("before recovery");
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM RECOVER", result);
        ASSERT_EQ(3, result.size());
    }
    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());
    {
        SCOPED_TRACE("after recovery");
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM RECOVER", result);
        ASSERT_EQ(3, result.size());
    }
    dump_content();
}

TEST_F(recovery_test, recovery_sequence_metadata) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory doesn't support recovery";
    }
    execute_statement("CREATE TABLE RECOVER (C0 INT, C1 INT)");
    execute_statement("INSERT INTO RECOVER (C0, C1) VALUES (1, 10)");
    execute_statement("INSERT INTO RECOVER (C0, C1) VALUES (2, 20)");
    execute_statement("INSERT INTO RECOVER (C0, C1) VALUES (3, 30)");
    {
        SCOPED_TRACE("before recovery");
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM RECOVER", result);
        ASSERT_EQ(3, result.size());
    }
    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());
    {
        SCOPED_TRACE("after recovery");
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM RECOVER", result);
        ASSERT_EQ(3, result.size());
        execute_statement("INSERT INTO RECOVER (C0, C1) VALUES (4, 40)", status::err_already_exists); // recoverying sequence metadata is not in place and this cause duplicate primary key TODO
    }
    dump_content();
}

// TODO recovery currently recovers deleted records
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
    wait_epochs(2);
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

TEST_F(recovery_test, recover_create_index) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory doesn't support recovery";
    }
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (2, 20)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (3, 30)");
    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());
    {
        auto t = std::make_shared<table>(
            "TEST",
            std::initializer_list<column>{
                column{ "C0", type::int8(), nullity{false} },
                column{ "C1", type::float8 (), nullity{true} },
            }
        );
        ASSERT_EQ(status::ok, db_->create_table(t));
        auto i = std::make_shared<yugawara::storage::index>(
            t,
            "TEST",
            std::initializer_list<index::key>{
                t->columns()[0],
            },
            std::initializer_list<index::column_ref>{
                t->columns()[1],
            },
            index_feature_set{
                ::yugawara::storage::index_feature::find,
                ::yugawara::storage::index_feature::scan,
                ::yugawara::storage::index_feature::unique,
                ::yugawara::storage::index_feature::primary,
            }
        );
        ASSERT_EQ(status::ok, db_->create_index(i));
    }
    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());
    {
        auto t = std::make_shared<table>(
            "TEST",
            std::initializer_list<column>{
                column{ "C0", type::int8(), nullity{false} },
                column{ "C1", type::float8 (), nullity{true} },
            }
        );
        ASSERT_EQ(status::ok, db_->create_table(t)); // recoverying metadata is not yet implemented //TODO
        auto i = std::make_shared<yugawara::storage::index>(
            t,
            "TEST",
            std::initializer_list<index::key>{
                t->columns()[0],
            },
            std::initializer_list<index::column_ref>{
                t->columns()[1],
            },
            index_feature_set{
                ::yugawara::storage::index_feature::find,
                ::yugawara::storage::index_feature::scan,
                ::yugawara::storage::index_feature::unique,
                ::yugawara::storage::index_feature::primary,
            }
        );
        ASSERT_EQ(status::ok, db_->create_index(i)); // recoverying metadata is not yet implemented //TODO
    }
}

TEST_F(recovery_test, recover_ddl) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory doesn't support recovery";
    }
    execute_statement("CREATE TABLE TEST (C0 INT NOT NULL PRIMARY KEY, C1 INT)");
    execute_statement("INSERT INTO TEST (C0, C1) VALUES (1, 10)");
    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM TEST", result);
        ASSERT_EQ(1, result.size());
    }
    execute_statement("DROP TABLE TEST");
    execute_statement("CREATE TABLE TEST (C0 INT NOT NULL PRIMARY KEY, C1 INT)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM TEST", result);
        ASSERT_EQ(0, result.size());
    }
}

// recoverying sequence is not in place yet TODO
TEST_F(recovery_test, DISABLED_recover_ddl_without_primary_key) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory doesn't support recovery";
    }
    execute_statement("CREATE TABLE TEST (C0 INT, C1 INT)");
    execute_statement("INSERT INTO TEST (C0, C1) VALUES (1, 10)");
    execute_statement("INSERT INTO TEST (C0, C1) VALUES (1, 10)");
    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());
    execute_statement("INSERT INTO TEST (C0, C1) VALUES (1, 10)");  //TODO insert fails as recoverying seq. fails
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM TEST", result);
        ASSERT_EQ(3, result.size());
    }
}
}
