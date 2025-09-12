/*
 * Copyright 2018-2024 Project Tsurugi.
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
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <gtest/gtest.h>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/api/executable_statement.h>
#include <jogasaki/api/field_type.h>
#include <jogasaki/api/field_type_kind.h>
#include <jogasaki/api/parameter_set.h>
#include <jogasaki/api/record_meta.h>
#include <jogasaki/api/statement_handle.h>
#include <jogasaki/configuration.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/executor/to_common_columns.h>
#include <jogasaki/status.h>
#include <jogasaki/utils/tables.h>
#include "jogasaki/utils/command_utils.h"

#include "api_test_base.h"

namespace jogasaki::api {
using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace std::chrono_literals;

/**
 * @brief test database api
 */
class result_metadata_test :
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

    auto get_result_meta(std::string_view sql) {
        statement_handle handle{};
        EXPECT_EQ(status::ok, db_->prepare(sql, handle));
        EXPECT_TRUE(handle);
        auto ps = api::create_parameter_set();
        std::unique_ptr<api::executable_statement> executable{};
        EXPECT_EQ(status::ok, db_->resolve(handle, std::shared_ptr{std::move(ps)}, executable));
        EXPECT_TRUE(executable);
        return static_cast<api::impl::record_meta const&>(*executable->meta()).external_meta();
    }
};

using atom_type = executor::dto::common_column::atom_type;

TEST_F(result_metadata_test, simple) {
    execute_statement("create table t (c0 int primary key)");
    auto meta = get_result_meta("select * from t");
    ASSERT_TRUE(meta);
    auto columns = executor::to_common_columns(*meta);
    std::vector<executor::dto::common_column> exp{
        {"c0", atom_type::int4, std::nullopt}
    };
    ASSERT_EQ(exp, columns);
}

TEST_F(result_metadata_test, ints) {
    execute_statement("create table t (c0 int primary key, c1 bigint)");
    std::vector<executor::dto::common_column> exp{
        {"c0", atom_type::int4, std::nullopt},
        {"c1", atom_type::int8, std::nullopt}
    };
    {
        auto meta = get_result_meta("select * from t");
        ASSERT_TRUE(meta);
        auto columns = executor::to_common_columns(*meta);
        ASSERT_EQ(exp, columns);
    }
    {
        auto meta = get_result_meta("select cast('' as int) c0, cast('' as bigint) c1 from t");
        ASSERT_TRUE(meta);
        auto columns = executor::to_common_columns(*meta);
        ASSERT_EQ(exp, columns);
    }
}

TEST_F(result_metadata_test, floats) {
    execute_statement("create table t (c0 real, c1 double)");
    std::vector<executor::dto::common_column> exp{
        {"c0", atom_type::float4, std::nullopt},
        {"c1", atom_type::float8, std::nullopt}
    };
    {
        auto meta = get_result_meta("select * from t");
        ASSERT_TRUE(meta);
        auto columns = executor::to_common_columns(*meta);
        ASSERT_EQ(exp, columns);
    }
    {
        auto meta = get_result_meta("select cast('' as real) c0, cast('' as double) c1 from t");
        ASSERT_TRUE(meta);
        auto columns = executor::to_common_columns(*meta);
        ASSERT_EQ(exp, columns);
    }
}

TEST_F(result_metadata_test, chars) {
    execute_statement("create table t (c0 varchar(5), c1 char(3), c2 varchar(*), c3 char)");
    std::vector<executor::dto::common_column> exp{
        {"c0", atom_type::character, std::nullopt, 5u},
        {"c1", atom_type::character, std::nullopt, 3u},
        {"c2", atom_type::character, std::nullopt, true},
        {"c3", atom_type::character, std::nullopt, 1u},
    };
    exp[0].varying_ = true;
    exp[1].varying_ = false;
    exp[2].varying_ = true;
    exp[3].varying_ = false;
    {
        auto meta = get_result_meta("select * from t");
        ASSERT_TRUE(meta);
        auto columns = executor::to_common_columns(*meta);
        ASSERT_EQ(exp, columns);
    }
    {
        auto meta = get_result_meta("select cast('' as varchar(5)) c0, cast('' as char(3)) c1, cast('' as varchar(*)) c2, cast('' as char) c3 from t");
        ASSERT_TRUE(meta);
        auto columns = executor::to_common_columns(*meta);
        ASSERT_EQ(exp, columns);
    }
}

TEST_F(result_metadata_test, concat_chars) {
    execute_statement("create table t (c0 char(5), c1 char(3), c2 varchar(5), c3 varchar(3), c4 varchar(*))");
    {
        // concat sums lengths and becomes varying
        auto meta = get_result_meta("select c0 || c1, c2 || c3, c0 || c3, c2 || c1 from t");
        ASSERT_TRUE(meta);
        auto columns = executor::to_common_columns(*meta);
        std::vector<executor::dto::common_column> exp{
            {"", atom_type::character, std::nullopt, 8u},  // char(5) + char(3) -> varchar(8)
            {"", atom_type::character, std::nullopt, 8u},  // varchar(5) + varchar(3) -> varchar(8)
            {"", atom_type::character, std::nullopt, 8u},  // char(5) + varchar(3) -> varchar(8)
            {"", atom_type::character, std::nullopt, 8u},  // varchar(5) + char(3) -> varchar(8)
        };
        exp[0].varying_ = true;
        exp[1].varying_ = true;
        exp[2].varying_ = true;
        exp[3].varying_ = true;
        ASSERT_EQ(exp, columns);
    }
    {
        // concat with varchar(*) becomes varchar(*)
        auto meta = get_result_meta("select c0 || c4, c4 || c0, c2 || c4, c4 || c2 from t");
        ASSERT_TRUE(meta);
        auto columns = executor::to_common_columns(*meta);
        std::vector<executor::dto::common_column> exp{
            {"", atom_type::character, std::nullopt, true},  // char(5) + varchar(*) -> varchar(*)
            {"", atom_type::character, std::nullopt, true},  // varchar(*) + char(5) -> varchar(*)
            {"", atom_type::character, std::nullopt, true},  // varchar(5) + varchar(*) -> varchar(*)
            {"", atom_type::character, std::nullopt, true},  // varchar(*) + varchar(5) -> varchar(*)
        };
        exp[0].varying_ = true;
        exp[1].varying_ = true;
        exp[2].varying_ = true;
        exp[3].varying_ = true;
        ASSERT_EQ(exp, columns);
    }
}

TEST_F(result_metadata_test, octets) {
    execute_statement("create table t (c0 varbinary(5), c1 binary(3), c2 varbinary(*), c3 binary)");
    std::vector<executor::dto::common_column> exp{
        {"c0", atom_type::octet, std::nullopt, 5u},
        {"c1", atom_type::octet, std::nullopt, 3u},
        {"c2", atom_type::octet, std::nullopt, true},
        {"c3", atom_type::octet, std::nullopt, 1u},
    };
    exp[0].varying_ = true;
    exp[1].varying_ = false;
    exp[2].varying_ = true;
    exp[3].varying_ = false;
    {
        auto meta = get_result_meta("select * from t");
        ASSERT_TRUE(meta);
        auto columns = executor::to_common_columns(*meta);
        ASSERT_EQ(exp, columns);
    }
    {
        auto meta = get_result_meta("select cast('' as varbinary(5)) c0, cast('' as binary(3)) c1, cast('' as varbinary(*)) c2, cast('' as binary) c3 from t");
        ASSERT_TRUE(meta);
        auto columns = executor::to_common_columns(*meta);
        ASSERT_EQ(exp, columns);
    }
}

TEST_F(result_metadata_test, concat_octets) {
    // same as concat_chars except uisng octets
    execute_statement("create table t (c0 binary(5), c1 binary(3), c2 varbinary(5), c3 varbinary(3), c4 varbinary(*))");
    {
        // concat sums lengths and becomes varying
        auto meta = get_result_meta("select c0 || c1, c2 || c3, c0 || c3, c2 || c1 from t");
        ASSERT_TRUE(meta);
        auto columns = executor::to_common_columns(*meta);
        std::vector<executor::dto::common_column> exp{
            {"", atom_type::octet, std::nullopt, 8u},  // binary(5) + binary(3) -> varbinary(8)
            {"", atom_type::octet, std::nullopt, 8u},  // varbinary(5) + varbinary(3) -> varbinary(8)
            {"", atom_type::octet, std::nullopt, 8u},  // binary(5) + varbinary(3) -> varbinary(8)
            {"", atom_type::octet, std::nullopt, 8u},  // varbinary(5) + binary(3) -> varbinary(8)
        };
        exp[0].varying_ = true;
        exp[1].varying_ = true;
        exp[2].varying_ = true;
        exp[3].varying_ = true;
        ASSERT_EQ(exp, columns);
    }
    {
        // concat with varbinary(*) becomes varbinary(*)
        auto meta = get_result_meta("select c0 || c4, c4 || c0, c2 || c4, c4 || c2 from t");
        ASSERT_TRUE(meta);
        auto columns = executor::to_common_columns(*meta);
        std::vector<executor::dto::common_column> exp{
            {"", atom_type::octet, std::nullopt, true},  // binary(5) + varbinary(*) -> varbinary(*)
            {"", atom_type::octet, std::nullopt, true},  // varbinary(*) + binary(5) -> varbinary(*)
            {"", atom_type::octet, std::nullopt, true},  // varbinary(5) + varbinary(*) -> varbinary(*)
            {"", atom_type::octet, std::nullopt, true},  // varbinary(*) + varbinary(5) -> varbinary(*)
        };
        exp[0].varying_ = true;
        exp[1].varying_ = true;
        exp[2].varying_ = true;
        exp[3].varying_ = true;
        ASSERT_EQ(exp, columns);
    }
}

TEST_F(result_metadata_test, decimals) {
    execute_statement("create table t (c0 decimal, c1 decimal(3), c2 decimal(5,3), c3 decimal(*, 5))");
    {
        auto meta = get_result_meta("select * from t");
        ASSERT_TRUE(meta);
        auto columns = executor::to_common_columns(*meta);
        std::vector<executor::dto::common_column> exp{
            {"c0", atom_type::decimal, std::nullopt, std::nullopt, 38u, 0u},
            {"c1", atom_type::decimal, std::nullopt, std::nullopt, 3u, 0u},
            {"c2", atom_type::decimal, std::nullopt, std::nullopt, 5u, 3u},
            {"c3", atom_type::decimal, std::nullopt, std::nullopt, 38u, 5u},
        };
        ASSERT_EQ(exp, columns);
    }
    {
        auto meta = get_result_meta("select cast('' as decimal) c0, cast('' as decimal(3)) c1, cast('' as decimal(5,3)) c2, cast('' as decimal(*, 5)) c3 from t");
        ASSERT_TRUE(meta);
        auto columns = executor::to_common_columns(*meta);
        std::vector<executor::dto::common_column> exp{
            {"c0", atom_type::decimal, std::nullopt, std::nullopt, 38u, 0u},
            {"c1", atom_type::decimal, std::nullopt, std::nullopt, 3u, 0u},
            {"c2", atom_type::decimal, std::nullopt, std::nullopt, 5u, 3u},
            {"c3", atom_type::decimal, std::nullopt, std::nullopt, true, 5u},  // ddl/runtime difference here //TODO issue #982
        };
        ASSERT_EQ(exp, columns);
    }
    {
        // DECIMAL(*,*) case only for runtime (cast) since ddl does not allow it.
        auto meta = get_result_meta("select cast('' as decimal(*,*)) c0 from t");
        ASSERT_TRUE(meta);
        auto columns = executor::to_common_columns(*meta);
        std::vector<executor::dto::common_column> exp{
                {"c0", atom_type::decimal, std::nullopt, std::nullopt, true, true},
            };
        ASSERT_EQ(exp, columns);
    }
}

TEST_F(result_metadata_test, calculate_decimals) {
    execute_statement("create table t (c0 decimal(5,3), c1 decimal(6,2))");
    {
        // add/subtraction -> decimal(*, max_of_scales)
        auto meta = get_result_meta("select c0+c1, c0-c1, c1+c0, c1-c0 from t");
        ASSERT_TRUE(meta);
        auto columns = executor::to_common_columns(*meta);
        std::vector<executor::dto::common_column> exp{
            {"", atom_type::decimal, std::nullopt, std::nullopt, true, 3u},
            {"", atom_type::decimal, std::nullopt, std::nullopt, true, 3u},
            {"", atom_type::decimal, std::nullopt, std::nullopt, true, 3u},
            {"", atom_type::decimal, std::nullopt, std::nullopt, true, 3u},
        };
        ASSERT_EQ(exp, columns);
    }
    {
        // multiply/division -> decimal(*, *)
        auto meta = get_result_meta("select c0*c1, c0/c1, c1*c0, c1/c0 from t");
        ASSERT_TRUE(meta);
        auto columns = executor::to_common_columns(*meta);
        std::vector<executor::dto::common_column> exp{
            {"", atom_type::decimal, std::nullopt, std::nullopt, true, true},
            {"", atom_type::decimal, std::nullopt, std::nullopt, true, true},
            {"", atom_type::decimal, std::nullopt, std::nullopt, true, true},
            {"", atom_type::decimal, std::nullopt, std::nullopt, true, true},
        };
        ASSERT_EQ(exp, columns);
    }
    {
        // decimal(5,3) + int -> decimal(5,3) + decimal(9,0) -> decimal(*,3)
        // decimal(5,3) + decimal(*,*) -> decimal(*,*)
        auto meta = get_result_meta("select c0+1, 1+c0, cast(c1 as decimal(*,*))+c0, c0+cast(c1 as decimal(*,*)) from t");
        ASSERT_TRUE(meta);
        auto columns = executor::to_common_columns(*meta);
        std::vector<executor::dto::common_column> exp{
            {"", atom_type::decimal, std::nullopt, std::nullopt, true, 3u},
            {"", atom_type::decimal, std::nullopt, std::nullopt, true, 3u},
            {"", atom_type::decimal, std::nullopt, std::nullopt, true, true},
            {"", atom_type::decimal, std::nullopt, std::nullopt, true, true},
        };
        ASSERT_EQ(exp, columns);
    }
}

}
