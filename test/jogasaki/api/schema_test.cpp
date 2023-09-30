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
#include <takatori/type/int.h>
#include <takatori/type/float.h>
#include <takatori/type/character.h>
#include <takatori/type/date.h>
#include <takatori/type/time_of_day.h>
#include <takatori/type/time_point.h>
#include <takatori/value/int.h>
#include <takatori/value/float.h>
#include <takatori/value/character.h>

#include <jogasaki/executor/common/graph.h>
#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/data/any.h>

#include <jogasaki/mock/basic_record.h>
#include <jogasaki/utils/storage_data.h>
#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/impl/record.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/meta/field_type_kind.h>
#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;

using takatori::util::unsafe_downcast;

using namespace yugawara::storage;
namespace type = takatori::type;
namespace value = takatori::value;
using nullity = yugawara::variable::nullity;
using kind = meta::field_type_kind;
using accessor::text;

using date_v = takatori::datetime::date;
using time_of_day_v = takatori::datetime::time_of_day;
using time_point_v = takatori::datetime::time_point;
using decimal_v = takatori::decimal::triple;

class schema_test :
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
    }

    void TearDown() override {
        db_teardown();
    }
};

using namespace std::string_view_literals;

TEST_F(schema_test, variety_types) {
    auto t = std::make_shared<table>(
        "TEST",
        std::initializer_list<column>{
            column{ "C0", type::int8(), nullity{false} },
            column{ "K1", type::character(type::varying), nullity{false} },
            column{ "K2", type::int8(), nullity{false} },
            column{ "K3", type::float8 (), nullity{false} },
            column{ "K4", type::character(~type::varying), nullity{false} },
            column{ "V1", type::character(type::varying), nullity{false} },
            column{ "V2", type::int8(), nullity{false} },
            column{ "V3", type::float8 (), nullity{false} },
            column{ "V4", type::character(~type::varying), nullity{false} },
            column{ "V5", type::int4(), nullity{true} },
            column{ "V6", type::float4(), nullity{true} },
        }
    );
    ASSERT_EQ(status::ok, db_->create_table(t));
    auto i = std::make_shared<yugawara::storage::index>(
        t,
        t->simple_name(),
        std::initializer_list<index::key>{
            t->columns()[0],
            t->columns()[1],
            t->columns()[2],
            t->columns()[3],
            t->columns()[4],
        },
        std::initializer_list<index::column_ref>{
            t->columns()[5],
            t->columns()[6],
            t->columns()[7],
            t->columns()[8],
            t->columns()[9],
            t->columns()[10],
        },
        index_feature_set{
            ::yugawara::storage::index_feature::find,
            ::yugawara::storage::index_feature::scan,
            ::yugawara::storage::index_feature::unique,
            ::yugawara::storage::index_feature::primary,
        }
    );
    ASSERT_EQ(status::ok, db_->create_index(i));

    execute_statement("INSERT INTO TEST VALUES (0, '0', 0, 0.0, '0', '0', 0, 0.0, '0', 0, 0.0)");
    execute_statement("INSERT INTO TEST VALUES (1, '1', 1, 1.0, '1', '1', 1, 1.0, '1', 1, 1.0)");
    execute_statement("INSERT INTO TEST VALUES (2, '2', 2, 2.0, '2', '2', 2, 2.0, '2', 2, 2.0)");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT C0, K1, K2, K3, K4, V1, V2, V3, V4, V5, V6 FROM TEST "
                  "WHERE "
                  "K1 = '1' AND "
                  "K2 = 1   AND "
                  "K3 = 1.0 AND "
                  "K4 = '1' AND "
                  "V1 = '1' AND "
                  "V2 = 1   AND "
                  "V3 = 1.0 AND "
                  "V4 = '1' AND "
                  "V5 = 1 AND "
                  "V6 = 1 AND "
                  "C0 = 1 ", result);
    ASSERT_EQ(1, result.size());
    auto exp = mock::create_record<kind::int8, kind::character, kind::int8, kind::float8, kind::character, kind::character, kind::int8, kind::float8, kind::character, kind::int4, kind::float4>(
        boost::dynamic_bitset<std::uint64_t>{"00000000000"s},  // note right most is position 0
        std::forward_as_tuple(1, text("1"), 1, 1.0, text("1"), text("1"), 1, 1.0, text("1"), 1, 1.0),
        {false, false, false, false, false, false, false, false, false, false, false }
    );
    EXPECT_EQ(exp, result[0]);
}

TEST_F(schema_test, nullables) {
    auto t = std::make_shared<table>(
        "TEST",
        std::initializer_list<column>{
            column{ "C0", type::int8(), nullity{false} },
            column{ "V1", type::character(type::varying), nullity{true} },
            column{ "V2", type::int8(), nullity{true} },
            column{ "V3", type::float8 (), nullity{true} },
            column{ "V4", type::character(~type::varying), nullity{true} },
            column{ "V5", type::int4(), nullity{true} },
            column{ "V6", type::float4(), nullity{true} },
        }
    );
    ASSERT_EQ(status::ok, db_->create_table(t));
    auto i = std::make_shared<yugawara::storage::index>(
        t,
        t->simple_name(),
        std::initializer_list<index::key>{
            t->columns()[0],
        },
        std::initializer_list<index::column_ref>{
            t->columns()[1],
            t->columns()[2],
            t->columns()[3],
            t->columns()[4],
            t->columns()[5],
            t->columns()[6],
        },
        index_feature_set{
            ::yugawara::storage::index_feature::find,
            ::yugawara::storage::index_feature::scan,
            ::yugawara::storage::index_feature::unique,
            ::yugawara::storage::index_feature::primary,
        }
    );
    ASSERT_EQ(status::ok, db_->create_index(i));

    execute_statement("INSERT INTO TEST VALUES (3, NULL, NULL, NULL, NULL, NULL, NULL)");
    execute_statement("INSERT INTO TEST VALUES (0, '0', 0, 0.0, '0', 0, 0.0)");
    execute_statement("INSERT INTO TEST VALUES (1, '1', 1, 1.0, '1', 1, 1.0)");
    execute_statement("INSERT INTO TEST VALUES (2, '2', 2, 2.0, '2', 2, 2.0)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, V1, V2, V3, V4, V5, V6 FROM TEST WHERE "
                      "C0 = 3", result);
        ASSERT_EQ(1, result.size());
        auto exp = mock::create_nullable_record<kind::int8, kind::character, kind::int8, kind::float8, kind::character, kind::int4, kind::float4>(
            std::forward_as_tuple(3, text("3"), 3, 3.0, text("3"), 3, 3.0),
            {false, true, true, true, true, true, true}
        );
        EXPECT_EQ(exp, result[0]);
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, V1, V2, V3, V4, V5, V6 FROM TEST WHERE "
                      "V1 = '1' AND "
                      "V2 = 1   AND "
                      "V3 = 1.0 AND "
                      "V4 = '1' AND "
                      "V5 = 1 AND "
                      "V6 = 1 AND "
                      "C0 = 1", result);
        ASSERT_EQ(1, result.size());
        auto exp = mock::create_nullable_record<kind::int8, kind::character, kind::int8, kind::float8, kind::character, kind::int4, kind::float4>(
            std::forward_as_tuple(1, text("1"), 1, 1.0, text("1"), 1, 1.0),
            {false, false, false, false, false, false, false}
        );
        EXPECT_EQ(exp, result[0]);
    }
}

TEST_F(schema_test, descending_keys) {
    auto t = std::make_shared<table>(
        "TEST",
        std::initializer_list<column>{
            column{ "C0", type::int8(), nullity{false} },
            column{ "K1", type::character(type::varying), nullity{false} },
            column{ "K2", type::int8(), nullity{false} },
            column{ "K3", type::float8 (), nullity{false} },
            column{ "K4", type::character(~type::varying), nullity{false} },
            column{ "K5", type::int4(), nullity{false} },
            column{ "K6", type::float4(), nullity{false} },
            column{ "V1", type::character(type::varying), nullity{true} },
            column{ "V2", type::int8(), nullity{true} },
            column{ "V3", type::float8 (), nullity{true} },
            column{ "V4", type::character(~type::varying), nullity{true} },
            column{ "V5", type::int4(), nullity{true} },
            column{ "V6", type::float4(), nullity{true} },
        }
    );
    ASSERT_EQ(status::ok, db_->create_table(t));
    auto i = std::make_shared<yugawara::storage::index>(
        t,
        t->simple_name(),
        std::initializer_list<index::key>{
            {t->columns()[0], sort_direction::descendant},
            {t->columns()[1], sort_direction::descendant},
            {t->columns()[2], sort_direction::descendant},
            {t->columns()[3], sort_direction::descendant},
            {t->columns()[4], sort_direction::descendant},
            {t->columns()[5], sort_direction::descendant},
            {t->columns()[6], sort_direction::descendant},
        },
        std::initializer_list<index::column_ref>{
            t->columns()[7],
            t->columns()[8],
            t->columns()[9],
            t->columns()[10],
            t->columns()[11],
            t->columns()[12],
        },
        index_feature_set{
            ::yugawara::storage::index_feature::find,
            ::yugawara::storage::index_feature::scan,
            ::yugawara::storage::index_feature::unique,
            ::yugawara::storage::index_feature::primary,
        }
    );
    ASSERT_EQ(status::ok, db_->create_index(i));
    execute_statement("INSERT INTO TEST (C0, K1, K2, K3, K4, K5, K6, V1, V2, V3, V4, V5, V6) VALUES (3, '3', 3, 3.0, '3', 3, 3.0, NULL, NULL, NULL, NULL, NULL, NULL)");
    execute_statement("INSERT INTO TEST (C0, K1, K2, K3, K4, K5, K6, V1, V2, V3, V4, V5, V6) VALUES (0, '0', 0, 0.0, '0', 0, 0.0, '0', 0, 0.0, '0', 0, 0.0)");
    execute_statement("INSERT INTO TEST (C0, K1, K2, K3, K4, K5, K6, V1, V2, V3, V4, V5, V6) VALUES (1, '1', 1, 1.0, '1', 1, 1.0, '1', 1, 1.0, '1', 1, 1.0)");
    execute_statement("INSERT INTO TEST (C0, K1, K2, K3, K4, K5, K6, V1, V2, V3, V4, V5, V6) VALUES (2, '2', 2, 2.0, '2', 2, 2.0, '2', 2, 2.0, '2', 2, 2.0)");

    std::vector<mock::basic_record> result{};
    execute_query("SELECT C0, K1, K2, K3, K4, K5, K6, V1, V2, V3, V4, V5, V6 FROM TEST WHERE C0 = 1", result);
    ASSERT_EQ(1, result.size());
    auto exp = mock::create_nullable_record<kind::int8, kind::character, kind::int8, kind::float8, kind::character, kind::int4, kind::float4, kind::character, kind::int8, kind::float8, kind::character, kind::int4, kind::float4>(
        std::forward_as_tuple(1, text("1"), 1, 1.0, text("1"), 1, 1.0, text("1"), 1, 1.0, text("1"), 1, 1.0),
        {false, false, false, false, false, false, false, false, false, false, false, false, false}
    );
    EXPECT_EQ(exp, result[0]);
}

TEST_F(schema_test, descending_keys_ordering) {
    auto check_desc = [&](std::size_t ind, std::string_view key) {
        auto tabname = "TEST"s + std::to_string(ind);
        auto t = std::make_shared<table>(
            tabname.c_str(),
            std::initializer_list<column>{
                column{ "C0", type::int8(), nullity{false} },
                column{ "K1", type::character(type::varying), nullity{false} },
                column{ "K2", type::int8(), nullity{false} },
                column{ "K3", type::float8 (), nullity{false} },
                column{ "K4", type::character(~type::varying), nullity{false} },
                column{ "K5", type::int4(), nullity{false} },
                column{ "K6", type::float4(), nullity{false} },
                column{ "V1", type::character(type::varying), nullity{true} },
                column{ "V2", type::int8(), nullity{true} },
                column{ "V3", type::float8 (), nullity{true} },
                column{ "V4", type::character(~type::varying), nullity{true} },
                column{ "V5", type::int4(), nullity{true} },
                column{ "V6", type::float4(), nullity{true} },
            }
        );
        ASSERT_EQ(status::ok, db_->create_table(t));
        std::vector<index::column_ref> colrefs{};
        colrefs.reserve(t->columns().size());
        std::vector<index::key> keys{};
        keys.emplace_back(index::key{t->columns()[ind], sort_direction::descendant});
        for(std::size_t i=0, n=t->columns().size(); i<n;++i) {
            if(i != ind) {
                colrefs.emplace_back(t->columns()[i]);
            }
        }

        auto i = std::make_shared<yugawara::storage::index>(
            t,
            std::string{t->simple_name()},
            keys,
            colrefs,
            index_feature_set{
                ::yugawara::storage::index_feature::find,
                ::yugawara::storage::index_feature::scan,
                ::yugawara::storage::index_feature::unique,
                ::yugawara::storage::index_feature::primary,
            }
        );
        ASSERT_EQ(status::ok, db_->create_index(i));
        execute_statement("INSERT INTO "+tabname+" (C0, K1, K2, K3, K4, K5, K6, V1, V2, V3, V4, V5, V6) VALUES (3, '3', 3, 3.0, '3', 3, 3.0, NULL, NULL, NULL, NULL, NULL, NULL)");
        execute_statement("INSERT INTO "+tabname+" (C0, K1, K2, K3, K4, K5, K6, V1, V2, V3, V4, V5, V6) VALUES (0, '0', 0, 0.0, '0', 0, 0.0, '0', 0, 0.0, '0', 0, 0.0)");
        execute_statement("INSERT INTO "+tabname+" (C0, K1, K2, K3, K4, K5, K6, V1, V2, V3, V4, V5, V6) VALUES (1, '1', 1, 1.0, '1', 1, 1.0, '1', 1, 1.0, '1', 1, 1.0)");
        execute_statement("INSERT INTO "+tabname+" (C0, K1, K2, K3, K4, K5, K6, V1, V2, V3, V4, V5, V6) VALUES (2, '2', 2, 2.0, '2', 2, 2.0, '2', 2, 2.0, '2', 2, 2.0)");

        {
            std::vector<mock::basic_record> result{};
            execute_query("SELECT C0 FROM "+tabname, result); // order is not assured by this sql, but using for testing
            ASSERT_EQ(4, result.size());
            EXPECT_EQ((mock::create_nullable_record<kind::int8>(3)), result[0]);
            EXPECT_EQ((mock::create_nullable_record<kind::int8>(0)), result[3]);
        }
        {
            std::vector<mock::basic_record> result{};
            execute_query("SELECT C0 FROM "+tabname+" ORDER BY " + std::string{key} + " DESC", result);
            ASSERT_EQ(4, result.size());
            EXPECT_EQ((mock::create_nullable_record<kind::int8>(3)), result[0]);
            EXPECT_EQ((mock::create_nullable_record<kind::int8>(0)), result[3]);
        }
        {
            std::vector<mock::basic_record> result{};
            execute_query("SELECT C0 FROM "+tabname+"  ORDER BY " + std::string{key} + " ASC", result);
            ASSERT_EQ(4, result.size());
            EXPECT_EQ((mock::create_nullable_record<kind::int8>(0)), result[0]);
            EXPECT_EQ((mock::create_nullable_record<kind::int8>(3)), result[3]);
        }
    };
    check_desc(1, "K1");
    check_desc(2, "K2");
    check_desc(3, "K3");
    check_desc(4, "K4");
    check_desc(5, "K5");
    check_desc(6, "K6");
}

TEST_F(schema_test, ascending_keys_ordering) {
    auto check_desc = [&](std::size_t ind, std::string_view key) {
        auto tabname = "TEST"s + std::to_string(ind);
        auto t = std::make_shared<table>(
            tabname.c_str(),
            std::initializer_list<column>{
                column{ "C0", type::int8(), nullity{false} },
                column{ "K1", type::character(type::varying), nullity{false} },
                column{ "K2", type::int8(), nullity{false} },
                column{ "K3", type::float8 (), nullity{false} },
                column{ "K4", type::character(~type::varying), nullity{false} },
                column{ "K5", type::int4(), nullity{false} },
                column{ "K6", type::float4(), nullity{false} },
                column{ "V1", type::character(type::varying), nullity{true} },
                column{ "V2", type::int8(), nullity{true} },
                column{ "V3", type::float8 (), nullity{true} },
                column{ "V4", type::character(~type::varying), nullity{true} },
                column{ "V5", type::int4(), nullity{true} },
                column{ "V6", type::float4(), nullity{true} },
            }
        );
        ASSERT_EQ(status::ok, db_->create_table(t));
        std::vector<index::column_ref> colrefs{};
        colrefs.reserve(t->columns().size());
        std::vector<index::key> keys{};
        keys.emplace_back(index::key{t->columns()[ind], sort_direction::ascendant});
        for(std::size_t i=0, n=t->columns().size(); i<n;++i) {
            if(i != ind) {
                colrefs.emplace_back(t->columns()[i]);
            }
        }

        auto i = std::make_shared<yugawara::storage::index>(
            t,
            std::string{t->simple_name()},
            keys,
            colrefs,
            index_feature_set{
                ::yugawara::storage::index_feature::find,
                ::yugawara::storage::index_feature::scan,
                ::yugawara::storage::index_feature::unique,
                ::yugawara::storage::index_feature::primary,
            }
        );
        ASSERT_EQ(status::ok, db_->create_index(i));
        execute_statement("INSERT INTO "+tabname+" VALUES (3, '3', 3, 3.0, '3', 3, 3.0, NULL, NULL, NULL, NULL, NULL, NULL)");
        execute_statement("INSERT INTO "+tabname+" VALUES (0, '0', 0, 0.0, '0', 0, 0.0, '0', 0, 0.0, '0', 0, 0.0)");
        execute_statement("INSERT INTO "+tabname+" VALUES (1, '1', 1, 1.0, '1', 1, 1.0, '1', 1, 1.0, '1', 1, 1.0)");
        execute_statement("INSERT INTO "+tabname+" VALUES (2, '2', 2, 2.0, '2', 2, 2.0, '2', 2, 2.0, '2', 2, 2.0)");

        {
            std::vector<mock::basic_record> result{};
            execute_query("SELECT C0 FROM "+tabname, result); // order is not assured by this sql, but using for testing
            ASSERT_EQ(4, result.size());
            EXPECT_EQ((mock::create_nullable_record<kind::int8>(0)), result[0]);
            EXPECT_EQ((mock::create_nullable_record<kind::int8>(3)), result[3]);
        }
        {
            std::vector<mock::basic_record> result{};
            execute_query("SELECT C0 FROM "+tabname+" ORDER BY " + std::string{key} + " DESC", result);
            ASSERT_EQ(4, result.size());
            EXPECT_EQ((mock::create_nullable_record<kind::int8>(3)), result[0]);
            EXPECT_EQ((mock::create_nullable_record<kind::int8>(0)), result[3]);
        }
        {
            std::vector<mock::basic_record> result{};
            execute_query("SELECT C0 FROM "+tabname+"  ORDER BY " + std::string{key} + " ASC", result);
            ASSERT_EQ(4, result.size());
            EXPECT_EQ((mock::create_nullable_record<kind::int8>(0)), result[0]);
            EXPECT_EQ((mock::create_nullable_record<kind::int8>(3)), result[3]);
        }
    };
    check_desc(1, "K1");
    check_desc(2, "K2");
    check_desc(3, "K3");
    check_desc(4, "K4");
    check_desc(5, "K5");
    check_desc(6, "K6");
}

TEST_F(schema_test, default_value) {
    auto t = std::make_shared<table>(
        "TEST",
        std::initializer_list<column>{
            column{ "C0", type::int8(), nullity{false}, {column_value{value::int8{0}}}},
            column{ "K1", type::character(type::varying), nullity{true}, {column_value{value::character{"1"}}}},
            column{ "K2", type::int8(), nullity{true}, {column_value{value::int8{2}}}},
            column{ "K3", type::float8 (), nullity{true}, {column_value{value::float8{3.0}}}},
        }
    );
    ASSERT_EQ(status::ok, db_->create_table(t));
    auto i = std::make_shared<yugawara::storage::index>(
        t,
        t->simple_name(),
        std::initializer_list<index::key>{
            t->columns()[0],
        },
        std::initializer_list<index::column_ref>{
            t->columns()[1],
            t->columns()[2],
            t->columns()[3],
        },
        index_feature_set{
            ::yugawara::storage::index_feature::find,
            ::yugawara::storage::index_feature::scan,
            ::yugawara::storage::index_feature::unique,
            ::yugawara::storage::index_feature::primary,
        }
    );
    ASSERT_EQ(status::ok, db_->create_index(i));

    {
        execute_statement("INSERT INTO TEST (C0) VALUES (10)");
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, K1, K2, K3 FROM TEST WHERE C0=10", result);
        ASSERT_EQ(1, result.size());
        auto exp = mock::create_record<kind::int8, kind::character, kind::int8, kind::float8>(
            boost::dynamic_bitset<std::uint64_t>{"1111"s},  // note right most is position 0
            std::forward_as_tuple(10, text("1"), 2, 3.0),
            {false, false, false, false}
        );
        EXPECT_EQ(exp, result[0]);
    }
    {
        execute_statement("INSERT INTO TEST (K2) VALUES (20)");
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, K1, K2, K3 FROM TEST WHERE K2=20", result);
        ASSERT_EQ(1, result.size());
        auto exp = mock::create_record<kind::int8, kind::character, kind::int8, kind::float8>(
            boost::dynamic_bitset<std::uint64_t>{"1111"s},  // note right most is position 0
            std::forward_as_tuple(0, text("1"), 20, 3.0),
            {false, false, false, false}
        );
        EXPECT_EQ(exp, result[0]);
    }
}

TEST_F(schema_test, default_value_with_variety_of_types) {
    auto t = std::make_shared<table>(
        "TEST",
        std::initializer_list<column>{
            column{ "C0", type::int8(), nullity{false}, {column_value{value::int8{0}}}},
            column{ "K1", type::character(~type::varying, 5), nullity{true}, {column_value{value::character{"ABC"}}}},
            column{ "K2", type::int4(), nullity{true}, {column_value{value::int4{2}}}},
            column{ "K3", type::float4 (), nullity{true}, {column_value{value::float4{3.0}}}},
        }
    );
    ASSERT_EQ(status::ok, db_->create_table(t));
    auto i = std::make_shared<yugawara::storage::index>(
        t,
        t->simple_name(),
        std::initializer_list<index::key>{
            t->columns()[0],
        },
        std::initializer_list<index::column_ref>{
            t->columns()[1],
            t->columns()[2],
            t->columns()[3],
        },
        index_feature_set{
            ::yugawara::storage::index_feature::find,
            ::yugawara::storage::index_feature::scan,
            ::yugawara::storage::index_feature::unique,
            ::yugawara::storage::index_feature::primary,
        }
    );
    ASSERT_EQ(status::ok, db_->create_index(i));

    {
        execute_statement("INSERT INTO TEST (C0) VALUES (10)");
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, K1, K2, K3 FROM TEST WHERE C0=10", result);
        ASSERT_EQ(1, result.size());
        auto exp = mock::create_record<kind::int8, kind::character, kind::int4, kind::float4>(
            boost::dynamic_bitset<std::uint64_t>{"1111"s},  // note right most is position 0
            std::forward_as_tuple(10, text("ABC  "), 2, 3.0),
            {false, false, false, false}
        );
        EXPECT_EQ(exp, result[0]);
    }
    {
        execute_statement("INSERT INTO TEST (K2) VALUES (20)");
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, K1, K2, K3 FROM TEST WHERE K2=20", result);
        ASSERT_EQ(1, result.size());
        auto exp = mock::create_record<kind::int8, kind::character, kind::int4, kind::float4>(
            boost::dynamic_bitset<std::uint64_t>{"1111"s},  // note right most is position 0
            std::forward_as_tuple(0, text("ABC  "), 20, 3.0),
            {false, false, false, false}
        );
        EXPECT_EQ(exp, result[0]);
    }
}

// assigning default value with different type inserts wrong data TODO
TEST_F(schema_test, DISABLED_default_value_with_different_type) {
    auto t = std::make_shared<table>(
        "TEST",
        std::initializer_list<column>{
            column{"C0", type::int8(), nullity{false}, {column_value{value::int8{0}}}},
            column{"K1", type::character(type::varying), nullity{true}, {column_value{value::character{"1"}}}},
            column{"K2", type::int4(), nullity{true}, {column_value{value::int8{2}}}},
            column{"K3", type::float4(), nullity{true}, {column_value{value::float8{123.456}}}},
        }
    );
    ASSERT_EQ(status::ok, db_->create_table(t));
    auto i = std::make_shared<yugawara::storage::index>(
        t,
        t->simple_name(),
        std::initializer_list<index::key>{
            t->columns()[0],
        },
        std::initializer_list<index::column_ref>{
            t->columns()[1],
            t->columns()[2],
            t->columns()[3],
        },
        index_feature_set{
            ::yugawara::storage::index_feature::find,
            ::yugawara::storage::index_feature::scan,
            ::yugawara::storage::index_feature::unique,
            ::yugawara::storage::index_feature::primary,
        }
    );
    ASSERT_EQ(status::ok, db_->create_index(i));

    {
        execute_statement("INSERT INTO TEST (C0) VALUES (10)");
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, K1, K2, K3 FROM TEST WHERE C0=10", result);
        ASSERT_EQ(1, result.size());
        auto exp = mock::create_record<kind::int8, kind::character, kind::int4, kind::float4>(
            boost::dynamic_bitset<std::uint64_t>{"1111"s},  // note right most is position 0
            std::forward_as_tuple(0, text("1"), 20, 3.0),
            {false, false, false, false}
        );
        EXPECT_EQ(exp, result[0]);
    }
    {
        execute_statement("INSERT INTO TEST (K2) VALUES (20)");
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, K1, K2, K3 FROM TEST WHERE K2=20", result);
        ASSERT_EQ(1, result.size());
        auto exp = mock::create_record<kind::int8, kind::character, kind::int4, kind::float4>(
            boost::dynamic_bitset<std::uint64_t>{"1111"s},  // note right most is position 0
            std::forward_as_tuple(0, text("1"), 20, 3.0),
            {false, false, false, false}
        );
        EXPECT_EQ(exp, result[0]);
    }
}
TEST_F(schema_test, null_value) {
    auto t = std::make_shared<table>(
        "TEST",
        std::initializer_list<column>{
            column{ "C0", type::int8(), nullity{false}},
            column{ "K1", type::character(type::varying), nullity{true}},
            column{ "K2", type::int8(), nullity{true}},
            column{ "K3", type::float8 (), nullity{true}},
        }
    );
    ASSERT_EQ(status::ok, db_->create_table(t));
    auto i = std::make_shared<yugawara::storage::index>(
        t,
        t->simple_name(),
        std::initializer_list<index::key>{
            t->columns()[0],
        },
        std::initializer_list<index::column_ref>{
            t->columns()[1],
            t->columns()[2],
            t->columns()[3],
        },
        index_feature_set{
            ::yugawara::storage::index_feature::find,
            ::yugawara::storage::index_feature::scan,
            ::yugawara::storage::index_feature::unique,
            ::yugawara::storage::index_feature::primary,
        }
    );
    ASSERT_EQ(status::ok, db_->create_index(i));

    {
        execute_statement("INSERT INTO TEST (C0) VALUES (10)");
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, K1, K2, K3 FROM TEST", result);
        ASSERT_EQ(1, result.size());
        auto exp = mock::create_record<kind::int8, kind::character, kind::int8, kind::float8>(
            boost::dynamic_bitset<std::uint64_t>{"1111"s},  // note right most is position 0
            std::forward_as_tuple(10, text("-"), 0, 0.0),
            {false, true, true, true}
        );
        EXPECT_EQ(exp, result[0]);
        execute_statement("DELETE FROM TEST");
    }
    wait_epochs(2);
    {
        execute_statement("INSERT INTO TEST (C0, K2) VALUES (20, 20)");
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, K1, K2, K3 FROM TEST", result);
        ASSERT_EQ(1, result.size());
        auto exp = mock::create_record<kind::int8, kind::character, kind::int8, kind::float8>(
            boost::dynamic_bitset<std::uint64_t>{"1111"s},  // note right most is position 0
            std::forward_as_tuple(20, text(""), 20, 0.0),
            {false, true, false, true}
        );
        EXPECT_EQ(exp, result[0]);
    }
}

}
