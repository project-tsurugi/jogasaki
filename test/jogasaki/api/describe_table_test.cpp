/*
 * Copyright 2018-2025 Project Tsurugi.
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
#include <jogasaki/executor/describe.h>

#include <cstddef>
#include <initializer_list>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <vector>
#include <gtest/gtest.h>

#include <takatori/datetime/date.h>
#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/time_point.h>
#include <takatori/decimal/triple.h>
#include <takatori/util/downcast.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <yugawara/storage/basic_configurable_provider.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/api/field_type_kind.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/parameter_set.h>
#include <jogasaki/api/statement_handle.h>
#include <jogasaki/configuration.h>
#include <jogasaki/error/error_info.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/id.h>
#include <jogasaki/meta/decimal_field_option.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_traits.h>
#include <jogasaki/meta/time_of_day_field_option.h>
#include <jogasaki/meta/time_point_field_option.h>
#include <jogasaki/meta/type_helper.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/port.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/status.h>
#include <jogasaki/test_utils/secondary_index.h>

#include "api_test_base.h"

namespace jogasaki::executor {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::meta;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;
using namespace jogasaki::mock;

using date_v = takatori::datetime::date;
using time_of_day_v = takatori::datetime::time_of_day;
using time_point_v = takatori::datetime::time_point;
using decimal_v = takatori::decimal::triple;
using takatori::util::unsafe_downcast;
using kind = meta::field_type_kind;
using api::impl::get_impl;

class describe_table_test :
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
    }

    void TearDown() override {
        db_teardown();
    }
};

using namespace std::string_view_literals;
using atom_type = dto::common_column::atom_type;

TEST_F(describe_table_test, simple) {
    execute_statement("create table t (c0 int primary key, c1 bigint, c2 real, c3 double)");
    dto::describe_table dt{};
    std::shared_ptr<error::error_info> err{};
    auto st = describe("t", dt, err, {});
    ASSERT_EQ(status::ok, st);

    dto::describe_table exp{"t",
        {
                {"c0", atom_type::int4, false},
                {"c1", atom_type::int8, true},
                {"c2", atom_type::float4, true},
                {"c3", atom_type::float8, true},
            },
            {"c0"},
        };
    EXPECT_EQ(exp, dt);
}

TEST_F(describe_table_test, compound_pk) {
    execute_statement("create table t (c0 int, c1 int, c2 int, c3 int, primary key(c1, c0))");
    dto::describe_table dt{};
    std::shared_ptr<error::error_info> err{};
    auto st = describe("t", dt, err, {});
    ASSERT_EQ(status::ok, st);

    dto::describe_table exp{"t",
        {
            {"c0", atom_type::int4, false},
            {"c1", atom_type::int4, false},
            {"c2", atom_type::int4, true},
            {"c3", atom_type::int4, true},
        },
        {"c1", "c0"},
    };
    EXPECT_EQ(exp, dt);
}

TEST_F(describe_table_test, not_found) {
    dto::describe_table dt{};
    std::shared_ptr<error::error_info> err{};
    auto st = describe("DUMMY", dt, err, {});
    ASSERT_EQ(status::err_not_found, st);
    ASSERT_TRUE(err);
    EXPECT_EQ(error_code::target_not_found_exception, err->code());
}

TEST_F(describe_table_test, length_and_varying) {
    execute_statement("create table t (c0 int, c1 char(1), c2 varchar(2), c3 varchar(*), c4 binary(4), c5 varbinary(5), c6 varbinary(*))");
    dto::describe_table dt{};
    std::shared_ptr<error::error_info> err{};
    auto st = describe("t", dt, err, {});
    ASSERT_EQ(status::ok, st);

    dto::describe_table exp{"t",
        {
                {"c0", atom_type::int4, true},
                {"c1", atom_type::character, true, 1u},
                {"c2", atom_type::character, true, 2u},
                {"c3", atom_type::character, true, true},
                {"c4", atom_type::octet, true, 4u},
                {"c5", atom_type::octet, true, 5u},
                {"c6", atom_type::octet, true, true},
            },
            {},
        };
    // c0 int4 should not have varying info.
    exp.columns_[1].varying_ = false;
    exp.columns_[2].varying_ = true;
    exp.columns_[3].varying_ = true;
    exp.columns_[4].varying_ = false;
    exp.columns_[5].varying_ = true;
    exp.columns_[6].varying_ = true;
    EXPECT_EQ(exp, dt);
}

TEST_F(describe_table_test, precision_and_scale) {
    execute_statement("create table t (c0 int, c1 decimal(5, 3), c2 decimal(5), c3 decimal(*, 3))");
    dto::describe_table dt{};
    std::shared_ptr<error::error_info> err{};
    auto st = describe("t", dt, err, {});
    ASSERT_EQ(status::ok, st);

    dto::describe_table exp{"t",
        {
                    {"c0", atom_type::int4, true},
                    {"c1", atom_type::decimal, true, std::nullopt, 5u, 3u},
                    {"c2", atom_type::decimal, true, std::nullopt, 5u, 0u},
                    {"c3", atom_type::decimal, true, std::nullopt, 38u, 3u},
                },
                {},
            };
    EXPECT_EQ(exp, dt);
}

TEST_F(describe_table_test, temporal_types) {
    // verify with_offset is correctly reflected on the output schema
    execute_statement("create table t (c0 DATE, c1 TIME, c2 TIMESTAMP, c3 TIME WITH TIME ZONE, c4 TIMESTAMP WITH TIME ZONE)");
    dto::describe_table dt{};
    std::shared_ptr<error::error_info> err{};
    auto st = describe("t", dt, err, {});
    ASSERT_EQ(status::ok, st);

    dto::describe_table exp{"t",
        {
                {"c0", atom_type::date, true},
                {"c1", atom_type::time_of_day, true},
                {"c2", atom_type::time_point, true},
                {"c3", atom_type::time_of_day_with_time_zone, true},
                {"c4", atom_type::time_point_with_time_zone, true},
            },
            {},
        };
    EXPECT_EQ(exp, dt);
}

TEST_F(describe_table_test, blob_types) {
    // verify blob types is correctly reflected on the output schema
    execute_statement("create table t (c0 BLOB, c1 CLOB)");
    dto::describe_table dt{};
    std::shared_ptr<error::error_info> err{};
    auto st = describe("t", dt, err, {});
    ASSERT_EQ(status::ok, st);

    dto::describe_table exp{"t",
        {
                    {"c0", atom_type::blob, true},
                    {"c1", atom_type::clob, true},
                },
                {},
            };
    EXPECT_EQ(exp, dt);
}
TEST_F(describe_table_test, pkless_table) {
    // make sure generated pk column is not visible
    execute_statement("create table t (c0 INT)");
    dto::describe_table dt{};
    std::shared_ptr<error::error_info> err{};
    auto st = describe("t", dt, err, {});
    ASSERT_EQ(status::ok, st);

    dto::describe_table exp{"t",
        {
                        {"c0", atom_type::int4, true},
                    },
                    {},
                };
    EXPECT_EQ(exp, dt);
}

TEST_F(describe_table_test, description) {
    auto table_ddl = R"(
        /**
        * Example table t.
        * This is a test table.
        */
        CREATE TABLE t (

        /** The key column. */
        k INT PRIMARY KEY,

        /**
         * The value column.
         * column for value.
         */
        v INT

        )
    )";
    execute_statement(table_ddl);
    dto::describe_table dt{};
    std::shared_ptr<error::error_info> err{};
    auto st = describe("t", dt, err, {});
    ASSERT_EQ(status::ok, st);

    dto::describe_table exp{"t",
        {
                            {"k", atom_type::int4, false},
                            {"v", atom_type::int4, true},
                        },
                        {"k"},
                    };
    exp.description_ = "Example table t.\nThis is a test table.";
    exp.columns_[0].description_ = "The key column.";
    exp.columns_[1].description_ = "The value column.\ncolumn for value.";
    EXPECT_EQ(exp, dt);
}

}
