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

#include <regex>
#include <gtest/gtest.h>

#include <takatori/util/downcast.h>

#include <jogasaki/executor/common/graph.h>
#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/executor/process/impl/expression/any.h>

#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/utils/mock/storage_data.h>
#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/transaction.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/impl/record.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/executor/tables.h>

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;

using takatori::util::unsafe_downcast;

class sql_test : public ::testing::Test {
public:
    // change this flag to debug with explain
    constexpr static bool to_explain = false;

    void SetUp() {
        auto cfg = std::make_shared<configuration>();
        db_ = api::create_database(cfg);
        cfg->single_thread(true);
        db_->start();
        auto* db_impl = unsafe_downcast<api::impl::database>(db_.get());
        add_benchmark_tables(*db_impl->tables());
        register_kvs_storage(*db_impl->kvs_db(), *db_impl->tables());
    }

    void TearDown() {
        db_->stop();
    }

    void explain(api::executable_statement& stmt) {
        if (to_explain) {
            db_->explain(stmt, std::cout);
            std::cout << std::endl;
        }
    }
    void execute_query(std::string_view query, std::vector<mock::basic_record>& out) {
        std::unique_ptr<api::executable_statement> stmt{};
        ASSERT_EQ(status::ok, db_->create_executable(query, stmt));
        explain(*stmt);
        auto tx = db_->create_transaction();
        std::unique_ptr<api::result_set> rs{};
        ASSERT_EQ(status::ok, tx->execute(*stmt, rs));
        ASSERT_TRUE(rs);
        auto it = rs->iterator();
        while(it->has_next()) {
            auto* record = it->next();
            std::stringstream ss{};
            ss << *record;
            auto* rec_impl = unsafe_downcast<api::impl::record>(record);
            auto* meta_impl = unsafe_downcast<api::impl::record_meta>(rs->meta());
            out.emplace_back(rec_impl->ref(), meta_impl->meta());
            LOG(INFO) << ss.str();
        }
        rs->close();
        tx->commit();
    }

    void execute_statement(std::string_view query) {
        std::unique_ptr<api::executable_statement> stmt{};
        ASSERT_EQ(status::ok, db_->create_executable(query, stmt));
        explain(*stmt);
        auto tx = db_->create_transaction();
        ASSERT_EQ(status::ok, tx->execute(*stmt));
        ASSERT_EQ(status::ok, tx->commit());
    }

    std::unique_ptr<jogasaki::api::database> db_;
};

using namespace std::string_view_literals;

TEST_F(sql_test, cross_join) {
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (2, 20.0)");
    execute_statement( "INSERT INTO T10 (C0, C1) VALUES (3, 30.0)");
    execute_statement( "INSERT INTO T10 (C0, C1) VALUES (4, 40.0)");
    execute_statement( "INSERT INTO T10 (C0, C1) VALUES (5, 50.0)");

    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T0, T10", result);
    ASSERT_EQ(6, result.size());
}

TEST_F(sql_test, update_by_part_of_primary_key) {
    execute_statement( "INSERT INTO T20 (C0, C2, C4) VALUES (1, 100.0, '111')");
    execute_statement( "UPDATE T20 SET C2=200.0 WHERE C0=1");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT C0, C1, C2 FROM T20", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_EQ(1, rec.ref().get_value<std::int64_t>(rec.record_meta()->value_offset(0)));
    EXPECT_TRUE(rec.ref().is_null(rec.record_meta()->nullity_offset(1)));
    EXPECT_DOUBLE_EQ(200.0, rec.ref().get_value<double>(rec.record_meta()->value_offset(2)));
    EXPECT_FALSE(rec.ref().is_null(rec.record_meta()->nullity_offset(2)));
}

// shirakami raises err_aborted_retryable on commit
TEST_F(sql_test, DISABLED_update_primary_key) {
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (2, 20.0)");
    execute_statement( "UPDATE T0 SET C0=3, C1=30.0 WHERE C1=10.0");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT C0, C1 FROM T0 ORDER BY C0", result);
    ASSERT_EQ(2, result.size());
    auto meta = result[0].record_meta();
    EXPECT_EQ(2, result[0].ref().get_value<std::int64_t>(meta->value_offset(0)));
    EXPECT_DOUBLE_EQ(20.0, result[0].ref().get_value<double>(meta->value_offset(1)));
    EXPECT_EQ(3, result[1].ref().get_value<std::int64_t>(meta->value_offset(0)));
    EXPECT_DOUBLE_EQ(30.0, result[1].ref().get_value<double>(meta->value_offset(1)));
}

TEST_F(sql_test, count_empty_records) {
    std::vector<mock::basic_record> result{};
    execute_query("SELECT COUNT(C1) FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_EQ(0, rec.ref().get_value<std::int64_t>(rec.record_meta()->value_offset(0)));
}

}
