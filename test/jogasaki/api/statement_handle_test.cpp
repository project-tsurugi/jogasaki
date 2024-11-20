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
#include <memory>
#include <string>
#include <unordered_map>
#include <gtest/gtest.h>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/api/field_type.h>
#include <jogasaki/api/field_type_kind.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/record_meta.h>
#include <jogasaki/api/statement_handle.h>
#include <jogasaki/configuration.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/status.h>
#include <jogasaki/utils/tables.h>

#include "api_test_base.h"

namespace jogasaki::api {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace std::chrono_literals;

/**
 * @brief test database api
 */
class statement_handle_test :
    public ::testing::Test,
    public testing::api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        cfg->prepare_test_tables(true);
        db_setup(cfg);

        auto* impl = db_impl();
        utils::add_benchmark_tables(*impl->tables());
        testing::register_kvs_storage(*impl->kvs_db(), *impl->tables());
    }

    void TearDown() override {
        db_teardown();
    }
};

TEST_F(statement_handle_test, meta) {
    std::string sql = "select C0, C1, C2, C3, C4 from T1";
    statement_handle handle{};
    ASSERT_EQ(status::ok, db_->prepare(sql, handle));
    ASSERT_TRUE(handle);
    ASSERT_TRUE(handle.meta());
    auto& meta = *handle.meta();
    ASSERT_EQ(5, meta.field_count());
    EXPECT_EQ(api::field_type_kind::int4, meta.at(0).kind());
    EXPECT_EQ(api::field_type_kind::int8, meta.at(1).kind());
    EXPECT_EQ(api::field_type_kind::float8, meta.at(2).kind());
    EXPECT_EQ(api::field_type_kind::float4, meta.at(3).kind());
    EXPECT_EQ(api::field_type_kind::character, meta.at(4).kind());
    ASSERT_EQ(status::ok, db_->destroy_statement(handle));
}

TEST_F(statement_handle_test, meta_with_parameters) {
    std::string sql = "select C0, C1, C2, C3, C4 from T1 where C0=:p0";
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::int8},
    };
    statement_handle handle{};
    ASSERT_EQ(status::ok, db_->prepare(sql, variables, handle));
    ASSERT_TRUE(handle);
    ASSERT_TRUE(handle.meta());
    auto& meta = *handle.meta();
    ASSERT_EQ(5, meta.field_count());
    EXPECT_EQ(api::field_type_kind::int4, meta.at(0).kind());
    EXPECT_EQ(api::field_type_kind::int8, meta.at(1).kind());
    EXPECT_EQ(api::field_type_kind::float8, meta.at(2).kind());
    EXPECT_EQ(api::field_type_kind::float4, meta.at(3).kind());
    EXPECT_EQ(api::field_type_kind::character, meta.at(4).kind());
    ASSERT_EQ(status::ok, db_->destroy_statement(handle));
}

TEST_F(statement_handle_test, empty_meta_from_prepared_statement) {
    {
        std::string sql = "insert into T0(C0, C1) values (1,1.0)";
        statement_handle handle{};
        ASSERT_EQ(status::ok, db_->prepare(sql, handle));
        ASSERT_TRUE(handle);
        ASSERT_FALSE(handle.meta());
        ASSERT_EQ(status::ok, db_->destroy_statement(handle));
    }
    {
        std::string sql = "update T0 set C0=2";
        statement_handle handle{};
        ASSERT_EQ(status::ok, db_->prepare(sql, handle));
        ASSERT_TRUE(handle);
        ASSERT_FALSE(handle.meta());
        ASSERT_EQ(status::ok, db_->destroy_statement(handle));
    }
}

TEST_F(statement_handle_test, empty_meta_with_parameters) {
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::int8},
        {"p1", api::field_type_kind::float8},
    };
    {
        std::string sql = "insert into T0(C0, C1) values (:p0,:p1)";
        statement_handle handle{};
        ASSERT_EQ(status::ok, db_->prepare(sql, variables, handle));
        ASSERT_TRUE(handle);
        ASSERT_FALSE(handle.meta());
        ASSERT_EQ(status::ok, db_->destroy_statement(handle));
    }
    {
        std::string sql = "update T0 set C0=:p0 where C1=:p1";
        statement_handle handle{};
        ASSERT_EQ(status::ok, db_->prepare(sql, variables, handle));
        ASSERT_TRUE(handle);
        ASSERT_FALSE(handle.meta());
        ASSERT_EQ(status::ok, db_->destroy_statement(handle));
    }
}

}  // namespace jogasaki::api
