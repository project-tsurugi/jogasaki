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

#include <regex>
#include <gtest/gtest.h>

#include <takatori/util/downcast.h>
#include <takatori/type/int.h>
#include <takatori/type/float.h>
#include <takatori/type/character.h>
#include <takatori/value/int.h>
#include <takatori/value/float.h>
#include <takatori/value/character.h>

#include <jogasaki/executor/common/graph.h>
#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/data/any.h>

#include <jogasaki/meta/type_helper.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/utils/storage_data.h>
#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/impl/record.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/executor/tables.h>
#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::meta;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;
using namespace jogasaki::mock;

using decimal_v = takatori::decimal::triple;
using takatori::util::unsafe_downcast;
using index = yugawara::storage::index;

using kind = meta::field_type_kind;

class secondary_index_types_test :
    public ::testing::Test,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        cfg->enable_index_join(true);
        db_setup(cfg);
    }

    void TearDown() override {
        db_teardown();
    }
};

using namespace std::string_view_literals;

bool contains(std::string_view whole, std::string_view part) {
    return whole.find(part) != std::string_view::npos;
}

TEST_F(secondary_index_types_test, find_by_char_column) {
    // verify find op on char column on secondary index
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 CHAR(5))");
    execute_statement("CREATE INDEX I ON T (C1)");
    execute_statement("INSERT INTO T (C0, C1) VALUES(1,'1')");
    execute_statement("INSERT INTO T (C0, C1) VALUES(2,'123')");
    execute_statement("INSERT INTO T (C0, C1) VALUES(3,'12345')");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T WHERE C1='123'", result);
        ASSERT_EQ(0, result.size());
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T WHERE C1='123  '", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((typed_nullable_record<kind::int4, kind::character>(
            std::tuple{int4_type(), character_type(false, 5)},
            std::forward_as_tuple(2,accessor::text{"123  "}))), result[0]);
    }
    {
        std::string plan{};
        explain_statement("SELECT * FROM T WHERE C1='123  '", plan);
        // verify 1. find op exists 2. with target index I
        EXPECT_TRUE(contains(plan,
            R"({"kind":"find","this":"@2","source":{"kind":"relation","binding":{"kind":"index","table":"T","simple_name":"I")"
        ));
    }
}

TEST_F(secondary_index_types_test, scan_by_char_column) {
    // verify scan op on char column on secondary index
    using namespace yugawara::storage;
    namespace type = takatori::type;
    namespace value = takatori::value;
    using nullity = yugawara::variable::nullity;
    using kind = meta::field_type_kind;
    using accessor::text;

    namespace type = ::takatori::type;
    using ::yugawara::variable::nullity;
    yugawara::storage::index_feature_set index_features{
        ::yugawara::storage::index_feature::find,
        ::yugawara::storage::index_feature::scan,
        ::yugawara::storage::index_feature::unique,
        ::yugawara::storage::index_feature::primary,
    };
    yugawara::storage::index_feature_set secondary_index_features{
        // to disable find
//        ::yugawara::storage::index_feature::find,
        ::yugawara::storage::index_feature::scan,
    };

    auto table_ = std::make_shared<table>(
        "CHARTAB",
        std::initializer_list<column>{
            column{"C0", type::int4(), nullity{false}},
            column{"C1", type::character(5), nullity{true}},
        }
    );
    ASSERT_EQ(status::ok, db_->create_table(table_));
    {
        std::shared_ptr<yugawara::storage::index> i = std::make_shared<yugawara::storage::index>(
            table_,
            table_->simple_name(),
            std::initializer_list<index::key>{
                table_->columns()[0],
            },
            std::initializer_list<index::column_ref>{
                table_->columns()[1],
            },
            index_features
        );
        ASSERT_EQ(status::ok, db_->create_index(i));
    }
    {
        auto i = std::make_shared<yugawara::storage::index>(
            table_,
            "I_CHARTAB_C1",
            std::initializer_list<index::key>{
                {table_->columns()[1], takatori::relation::sort_direction::ascendant},
            },
            std::initializer_list<index::column_ref>{},
            secondary_index_features
        );
        ASSERT_EQ(status::ok, db_->create_index(i));
    }
    execute_statement("INSERT INTO CHARTAB (C0, C1) VALUES(1,'1')");
    execute_statement("INSERT INTO CHARTAB (C0, C1) VALUES(2,'123')");
    execute_statement("INSERT INTO CHARTAB (C0, C1) VALUES(3,'12345')");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM CHARTAB WHERE C1='123'", result);
        ASSERT_EQ(0, result.size());
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM CHARTAB WHERE C1='123  '", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((typed_nullable_record<kind::int4, kind::character>(
            std::tuple{int4_type(), character_type(false, 5)},
            std::forward_as_tuple(2,accessor::text{"123  "}))), result[0]);
    }
    {
        std::string plan{};
        explain_statement("SELECT * FROM CHARTAB WHERE C1='123  '", plan);
        // verify 1. scan op exists 2. with target index I_CHARTAB_C1
        EXPECT_TRUE(contains(plan,
            R"({"kind":"scan","this":"@2","source":{"kind":"relation","binding":{"kind":"index","table":"CHARTAB","simple_name":"I_CHARTAB_C1")"
        ));
    }
}

TEST_F(secondary_index_types_test, join_find_by_char_column) {
    // verify find op on char column on secondary index
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 VARCHAR(5))");
    execute_statement("CREATE TABLE S (C0 INT NOT NULL PRIMARY KEY, C1 CHAR(5))");
    execute_statement("CREATE INDEX I ON S (C1)");
    execute_statement("INSERT INTO T (C0, C1) VALUES(2,'123')");
    execute_statement("INSERT INTO S (C0, C1) VALUES(20,'123')");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT T.C0, S.C0 FROM T, S WHERE T.C1=S.C1", result);
        ASSERT_EQ(0, result.size());
    }
    execute_statement("INSERT INTO T (C0, C1) VALUES(1,'123  ')");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT T.C0, S.C0 FROM T, S WHERE T.C1=S.C1", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(1,20)), result[0]);
    }
    {
        std::string plan{};
        explain_statement("SELECT T.C0, S.C0 FROM T, S WHERE T.C1=S.C1", plan);
        // verify 1. join_find op exists 2. with target index I
        EXPECT_TRUE(contains(plan,
            R"({"kind":"join_find","this":"@5","operator_kind":"inner","source":{"kind":"relation","binding":{"kind":"index","table":"S","simple_name":"I")"
        ));
    }
}

}
