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
#include <jogasaki/api/executable_statement.h>

#include <thread>
#include <gtest/gtest.h>
#include <glog/logging.h>

#include <jogasaki/api.h>
#include <jogasaki/test_utils.h>
#include <jogasaki/accessor/record_printer.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/scheduler/task_scheduler.h>
#include <jogasaki/executor/sequence/manager.h>
#include <jogasaki/executor/sequence/sequence.h>
#include "api_test_base.h"

namespace jogasaki::api {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace std::chrono_literals;

/**
 * @brief test database api
 */
class executable_statement_api_test :
    public ::testing::Test,
    public testing::api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        db_setup(cfg);

        auto* impl = db_impl();
        testing::add_benchmark_tables(*impl->tables());
        testing::register_kvs_storage(*impl->kvs_db(), *impl->tables());
    }

    void TearDown() override {
        db_teardown();
    }
};

TEST_F(executable_statement_api_test, meta) {
    std::string sql = "select C0, C1, C2, C3, C4 from T1";
    statement_handle handle{};
    ASSERT_EQ(status::ok, db_->prepare(sql, handle));
    ASSERT_TRUE(handle);
    auto ps = api::create_parameter_set();
    std::unique_ptr<api::executable_statement> executable{};
    ASSERT_EQ(status::ok, db_->resolve(handle, std::shared_ptr{std::move(ps)}, executable));
    ASSERT_TRUE(executable);
    ASSERT_TRUE(executable->meta());
    auto& meta = *executable->meta();
    ASSERT_EQ(5, meta.field_count());
    EXPECT_EQ(api::field_type_kind::int4, meta.at(0).kind());
    EXPECT_EQ(api::field_type_kind::int8, meta.at(1).kind());
    EXPECT_EQ(api::field_type_kind::float8, meta.at(2).kind());
    EXPECT_EQ(api::field_type_kind::float4, meta.at(3).kind());
    EXPECT_EQ(api::field_type_kind::character, meta.at(4).kind());
    ASSERT_EQ(status::ok, db_->destroy_statement(handle));
}

TEST_F(executable_statement_api_test, meta_with_create_executable_api) {
    std::string sql = "select C0, C1, C2, C3, C4 from T1";
    std::unique_ptr<api::executable_statement> executable{};
    ASSERT_EQ(status::ok, db_->create_executable(sql, executable));
    ASSERT_TRUE(executable);
    ASSERT_TRUE(executable->meta());
    auto& meta = *executable->meta();
    ASSERT_EQ(5, meta.field_count());
    EXPECT_EQ(api::field_type_kind::int4, meta.at(0).kind());
    EXPECT_EQ(api::field_type_kind::int8, meta.at(1).kind());
    EXPECT_EQ(api::field_type_kind::float8, meta.at(2).kind());
    EXPECT_EQ(api::field_type_kind::float4, meta.at(3).kind());
    EXPECT_EQ(api::field_type_kind::character, meta.at(4).kind());
}

TEST_F(executable_statement_api_test, meta_with_parameters) {
    std::string sql = "select C0, C1, C2, C3, C4 from T1 where C0=:p0";
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::int8},
    };
    statement_handle handle{};
    ASSERT_EQ(status::ok, db_->prepare(sql, variables, handle));
    std::unique_ptr<api::executable_statement> executable{};
    auto ps = api::create_parameter_set();
    set(*ps, "p0", api::field_type_kind::int8, 1);
    ASSERT_EQ(status::ok, db_->resolve(handle, std::shared_ptr{std::move(ps)}, executable));
    ASSERT_TRUE(executable);
    ASSERT_TRUE(executable->meta());
    auto& meta = *executable->meta();
    ASSERT_EQ(5, meta.field_count());
    EXPECT_EQ(api::field_type_kind::int4, meta.at(0).kind());
    EXPECT_EQ(api::field_type_kind::int8, meta.at(1).kind());
    EXPECT_EQ(api::field_type_kind::float8, meta.at(2).kind());
    EXPECT_EQ(api::field_type_kind::float4, meta.at(3).kind());
    EXPECT_EQ(api::field_type_kind::character, meta.at(4).kind());
    ASSERT_EQ(status::ok, db_->destroy_statement(handle));
}

TEST_F(executable_statement_api_test, empty_meta) {
    {
        std::string sql = "insert into T0(C0, C1) values (1,1.0)";
        statement_handle handle{};
        ASSERT_EQ(status::ok, db_->prepare(sql, handle));
        ASSERT_TRUE(handle);
        auto ps = api::create_parameter_set();
        std::unique_ptr<api::executable_statement> executable{};
        ASSERT_EQ(status::ok, db_->resolve(handle, std::shared_ptr{std::move(ps)}, executable));
        ASSERT_TRUE(executable);
        ASSERT_FALSE(executable->meta());
        ASSERT_EQ(status::ok, db_->destroy_statement(handle));
    }
    {
        std::string sql = "update T0 set C0=2";
        statement_handle handle{};
        ASSERT_EQ(status::ok, db_->prepare(sql, handle));
        ASSERT_TRUE(handle);
        auto ps = api::create_parameter_set();
        std::unique_ptr<api::executable_statement> executable{};
        ASSERT_EQ(status::ok, db_->resolve(handle, std::shared_ptr{std::move(ps)}, executable));
        ASSERT_TRUE(executable);
        ASSERT_FALSE(executable->meta());
        ASSERT_EQ(status::ok, db_->destroy_statement(handle));
    }
}

TEST_F(executable_statement_api_test, empty_meta_with_parameters) {
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::int8},
        {"p1", api::field_type_kind::float8},
    };
    {
        std::string sql = "insert into T0(C0, C1) values (:p0,:p1)";
        statement_handle handle{};
        ASSERT_EQ(status::ok, db_->prepare(sql, variables, handle));
        ASSERT_TRUE(handle);
        auto ps = api::create_parameter_set();
        set(*ps, "p0", api::field_type_kind::int8, 1);
        set(*ps, "p1", api::field_type_kind::float8, 1.0);
        std::unique_ptr<api::executable_statement> executable{};
        ASSERT_EQ(status::ok, db_->resolve(handle, *ps, executable));
        ASSERT_TRUE(executable);
        ASSERT_FALSE(executable->meta());
        ASSERT_EQ(status::ok, db_->destroy_statement(handle));
    }
    {
        std::string sql = "update T0 set C0=:p0 where C1=:p1";
        statement_handle handle{};
        ASSERT_EQ(status::ok, db_->prepare(sql, variables, handle));
        ASSERT_TRUE(handle);
        auto ps = api::create_parameter_set();
        set(*ps, "p0", api::field_type_kind::int8, 1);
        set(*ps, "p1", api::field_type_kind::float8, 1.0);
        auto& stmt = *handle.get();
        ASSERT_FALSE(stmt.meta());
        ASSERT_EQ(status::ok, db_->destroy_statement(handle));
    }
}

}
