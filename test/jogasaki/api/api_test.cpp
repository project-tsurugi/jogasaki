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

class api_test : public ::testing::Test {
public:
    // change this flag to debug with explain
    constexpr static bool to_explain = false;

    void SetUp() {
        auto cfg = std::make_shared<configuration>();
        db_ = api::create_database(cfg);
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

TEST_F(api_test, syntax_error) {
    std::unique_ptr<api::executable_statement> stmt{};
    ASSERT_EQ(status::err_parse_error, db_->create_executable("AAA", stmt));
}

TEST_F(api_test, missing_table) {
    std::unique_ptr<api::executable_statement> stmt{};
    ASSERT_EQ(status::err_translator_error, db_->create_executable("select * from dummy", stmt));
}

TEST_F(api_test, invalid_column_name) {
    std::unique_ptr<api::executable_statement> stmt{};
    ASSERT_EQ(status::err_translator_error, db_->create_executable("INSERT INTO T0(dummy) VALUES(1)", stmt));
}

TEST_F(api_test, inconsistent_type_in_write) {
    std::unique_ptr<api::executable_statement> stmt{};
    ASSERT_EQ(status::err_compiler_error, db_->create_executable("INSERT INTO T0(C0) VALUES('X')", stmt));
}

TEST_F(api_test, inconsistent_type_in_query) {
    std::unique_ptr<api::executable_statement> stmt{};
    ASSERT_EQ(status::err_compiler_error, db_->create_executable("select C1 from T0 where C1='X'", stmt));
}

TEST_F(api_test, primary_key_violation) {
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    std::unique_ptr<api::executable_statement> stmt{};
    ASSERT_EQ(status::ok, db_->create_executable("INSERT INTO T0 (C0, C1) VALUES (1, 20.0)", stmt));
    auto tx = db_->create_transaction();
    ASSERT_EQ(status::err_already_exists, tx->execute(*stmt));
    ASSERT_EQ(status::ok, tx->abort());

    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_EQ(1, rec.ref().get_value<std::int64_t>(rec.record_meta()->value_offset(0)));
    EXPECT_DOUBLE_EQ(10.0, rec.ref().get_value<double>(rec.record_meta()->value_offset(1)));
}

TEST_F(api_test, resolve_place_holder_with_null) {
    std::unique_ptr<api::prepared_statement> prepared{};
    ASSERT_EQ(status::ok, db_->prepare("INSERT INTO T0 (C0, C1) VALUES(:p1, :p2)", prepared));
    {
        auto tx = db_->create_transaction();
        auto ps = api::create_parameter_set();
        ps->set_int8("p1", 1);
        ps->set_null("p2");
        std::unique_ptr<api::executable_statement> exec{};
        ASSERT_EQ(status::ok,db_->resolve(*prepared, *ps, exec));
        ASSERT_EQ(status::ok,tx->execute(*exec));
        tx->commit();
    }

    std::vector<mock::basic_record> result{};
    execute_query("SELECT C0, C1 FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_EQ(1, rec.ref().get_value<std::int64_t>(rec.record_meta()->value_offset(0)));
    EXPECT_TRUE(rec.ref().is_null(rec.record_meta()->nullity_offset(1)));
}

TEST_F(api_test, dump_load) {
    execute_statement( "DELETE FROM T0");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (2,20.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1,10.0)");
    std::stringstream ss{};
    db_->dump(ss, "I0", 0);
    execute_statement( "DELETE FROM T0");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT C0, C1 FROM T0 ORDER BY C0", result);
    ASSERT_EQ(0, result.size());
    db_->load(ss, "I0", 0);
    execute_query("SELECT C0, C1 FROM T0 ORDER BY C0", result);
    ASSERT_EQ(2, result.size());
    auto meta = result[0].record_meta();
    EXPECT_EQ(1, result[0].ref().get_value<std::int64_t>(meta->value_offset(0)));
    EXPECT_DOUBLE_EQ(10.0, result[0].ref().get_value<double>(meta->value_offset(1)));
    EXPECT_EQ(2, result[1].ref().get_value<std::int64_t>(meta->value_offset(0)));
    EXPECT_DOUBLE_EQ(20.0, result[1].ref().get_value<double>(meta->value_offset(1)));
}

}
