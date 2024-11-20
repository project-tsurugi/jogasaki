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
#include <chrono>
#include <cstddef>
#include <iosfwd>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/api/executable_statement.h>
#include <jogasaki/api/field_type_kind.h>
#include <jogasaki/api/parameter_set.h>
#include <jogasaki/api/record.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/result_set_iterator.h>
#include <jogasaki/api/statement_handle.h>
#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/configuration.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/status.h>
#include <jogasaki/utils/create_tx.h>
#include <jogasaki/utils/tables.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace std::chrono_literals;

/**
 * @brief test database api
 */
class database_test :
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

        auto& impl = *db_impl();
        jogasaki::utils::add_test_tables(*impl.tables());
        jogasaki::executor::register_kvs_storage(*impl.kvs_db(), *impl.tables());
    }

    void TearDown() override {
        db_teardown();
    }
};

TEST_F(database_test, simple) {
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::int8},
        {"p1", api::field_type_kind::float8},
    };
    api::statement_handle prepared{};
    ASSERT_EQ(status::ok, db_->prepare("INSERT INTO T0 (C0, C1) VALUES(:p0, :p1)", variables, prepared));
    {
        auto tx = utils::create_transaction(*db_);
        for(std::size_t i=0; i < 2; ++i) {
            auto ps = api::create_parameter_set();
            ps->set_int8("p0", i);
            ps->set_float8("p1", 10.0*i);
            std::unique_ptr<api::executable_statement> exec{};
            ASSERT_EQ(status::ok,db_->resolve(prepared, std::shared_ptr{std::move(ps)}, exec));
            ASSERT_EQ(status::ok,tx->execute(*exec));
        }
        tx->commit();
    }
    ASSERT_EQ(status::ok,db_->destroy_statement(prepared));
    ASSERT_EQ(status::err_invalid_argument, db_->destroy_statement(prepared));

    {
        auto tx = utils::create_transaction(*db_);
        std::unique_ptr<api::executable_statement> exec{};
        ASSERT_EQ(status::ok,db_->create_executable("select * from T0 order by C0", exec));
        explain(*exec);
        std::unique_ptr<api::result_set> rs{};
        ASSERT_EQ(status::ok,tx->execute(*exec, rs));
        auto it = rs->iterator();
        std::size_t count = 0;
        while(it->has_next()) {
            std::stringstream ss{};
            auto* record = it->next();
            ss << *record;
            LOG(INFO) << ss.str();
            ++count;
        }
        EXPECT_EQ(2, count);
        tx->commit();
    }
    {
        // reuse prepared statement
        api::statement_handle prep{};
        ASSERT_EQ(status::ok,db_->prepare("select * from T0 where C0 = :p0", variables, prep));
        auto ps = std::shared_ptr{api::create_parameter_set()};
        ps->set_int8("p0", 0);
        std::unique_ptr<api::executable_statement> exec{};
        ASSERT_EQ(status::ok,db_->resolve(prep, ps, exec));
        explain(*exec);
        auto f = [&]() {
            auto tx = utils::create_transaction(*db_);
            std::unique_ptr<api::result_set> rs{};
            ASSERT_EQ(status::ok,tx->execute(*exec, rs));
            auto it = rs->iterator();
            std::size_t count = 0;
            while(it->has_next()) {
                std::stringstream ss{};
                auto* record = it->next();
                ss << *record;
                LOG(INFO) << ss.str();
                ++count;
            }
            EXPECT_EQ(1, count);
            tx->commit();
        };
        f();
        ps->set_int8("p0", 1);
        ASSERT_EQ(status::ok,db_->resolve(prep, ps, exec));
        ASSERT_EQ(status::ok,db_->destroy_statement(prep));
        ASSERT_EQ(status::err_invalid_argument, db_->destroy_statement(prep));
        ps.reset();
        f();
    }
}

TEST_F(database_test, update_with_host_variable) {
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p1", api::field_type_kind::float8},
    };
    api::statement_handle prepared{};
    ASSERT_EQ(status::ok, db_->prepare("UPDATE T0 SET C1 = :p1 WHERE C0 = 0", variables, prepared));
    std::unique_ptr<api::executable_statement> insert{};
    ASSERT_EQ(status::ok, db_->create_executable("INSERT INTO T0 (C0, C1) VALUES(0, 10.0)", insert));
    {
        auto tx = utils::create_transaction(*db_);
        ASSERT_EQ(status::ok,tx->execute(*insert));
        tx->commit();
    }
    {
        SCOPED_TRACE("update c1 to 0");
        auto tx = utils::create_transaction(*db_);
        auto ps = api::create_parameter_set();
        ps->set_float8("p1", 0.0);
        std::unique_ptr<api::executable_statement> exec{};
        ASSERT_EQ(status::ok,db_->resolve(prepared, std::shared_ptr{std::move(ps)}, exec));
        ASSERT_EQ(status::ok,tx->execute(*exec));
        tx->commit();
    }
    {
        SCOPED_TRACE("verify 0");
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C1 FROM T0 ORDER BY C0", result);
        ASSERT_EQ(1, result.size());
        auto meta = result[0].record_meta();
        auto c1 = result[0].ref().get_value<double>(meta->value_offset(0));
        EXPECT_FALSE(result[0].ref().is_null(meta->nullity_offset(0)));
        EXPECT_DOUBLE_EQ(0.0, c1);
    }
    {
        SCOPED_TRACE("update c1 to null");
        auto tx = utils::create_transaction(*db_);
        auto ps = api::create_parameter_set();
        ps->set_null("p1");
        std::unique_ptr<api::executable_statement> exec{};
        ASSERT_EQ(status::ok,db_->resolve(prepared, std::shared_ptr{std::move(ps)}, exec));
        ASSERT_EQ(status::ok,tx->execute(*exec));
        tx->commit();
    }
    {
        SCOPED_TRACE("verify null");
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C1 FROM T0 ORDER BY C0", result);
        ASSERT_EQ(1, result.size());
        auto meta = result[0].record_meta();
        EXPECT_TRUE(result[0].ref().is_null(meta->nullity_offset(0)));
    }
    ASSERT_EQ(status::ok,db_->destroy_statement(prepared));
}

TEST_F(database_test, long_transaction) {
    std::unique_ptr<api::executable_statement> insert0{}, insert1{};
    EXPECT_EQ(status::ok, db_->create_executable("INSERT INTO T0 (C0, C1) VALUES(0, 10.0)", insert0));
    EXPECT_EQ(status::ok, db_->create_executable("INSERT INTO T1 (C0, C1) VALUES(0, 10)", insert1));
    {
        auto tx = utils::create_transaction(*db_, false, true, {"T0", "T1"});
        EXPECT_EQ(status::ok,tx->execute(*insert0));
        EXPECT_EQ(status::ok,tx->execute(*insert1));
        EXPECT_EQ(status::ok,tx->commit());
    }
}
}
