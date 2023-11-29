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
using index = yugawara::storage::index;
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
    ::yugawara::storage::index_feature::find,
    ::yugawara::storage::index_feature::scan,
};

class secondary_index_upsert_test :
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

        table_ = std::make_shared<table>(
            "TEST",
            std::initializer_list<column>{
                column{ "C0", type::int8(), nullity{false} },
                column{ "K1", type::int8(), nullity{true} },
                column{ "K2", type::int8(), nullity{true} },
                column{ "V1", type::int8(), nullity{true} },
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
                table_->columns()[2],
                table_->columns()[3],
            },
            index_features
        );
        ASSERT_EQ(status::ok, db_->create_index(i));
        }
        {
            auto i = std::make_shared<yugawara::storage::index>(
                table_,
                "TEST_SECONDARY0",
                std::initializer_list<index::key>{
                    {table_->columns()[1], takatori::relation::sort_direction::ascendant},
                },
                std::initializer_list<index::column_ref>{},
                secondary_index_features
            );
            ASSERT_EQ(status::ok, db_->create_index(i));
        }
        {
            auto i = std::make_shared<yugawara::storage::index>(
                table_,
                "TEST_SECONDARY1",
                std::initializer_list<index::key>{
                    {table_->columns()[2], takatori::relation::sort_direction::descendant},
                },
                std::initializer_list<index::column_ref>{},
                secondary_index_features
            );
            ASSERT_EQ(status::ok, db_->create_index(i));
        }
    }

    void TearDown() override {
        db_teardown();
    }
    std::shared_ptr<table> table_{};
};

using namespace std::string_view_literals;

bool contains(std::string_view whole, std::string_view part) {
    return whole.find(part) != std::string_view::npos;
}

TEST_F(secondary_index_upsert_test, upsert_creates_new_entry_on_secondary_index) {
    // simple scenario INSERT OR REPLACE creates brand-new entry
    execute_statement("CREATE TABLE T(C0 INT PRIMARY KEY, C1 INT, C2 INT)");
    execute_statement("CREATE INDEX I ON T(C1)");
    execute_statement("INSERT OR REPLACE INTO T VALUES (1, 10, 100)");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT C0, C1, C2 FROM T WHERE C1 = 10 ORDER BY C0", result);
    ASSERT_EQ(1, result.size());
    ASSERT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 10, 100)), result[0]);
    {
        std::string plan{};
        explain_statement("SELECT C0, C1, C2 FROM T WHERE C1 = 10 ORDER BY C0", plan);
        // verify 1. find op exists 2. with target index I
        EXPECT_TRUE(contains(plan,
            R"({"kind":"find","this":"@2","source":{"kind":"relation","binding":{"kind":"index","table":"T","simple_name":"I")"
        ));
    }
}

TEST_F(secondary_index_upsert_test, upsert_updates_existing_entry_on_secondary_index) {
    // INSERT OR REPLACE replaces existing entry (delete+upsert on secondary index)
    execute_statement("CREATE TABLE T(C0 INT PRIMARY KEY, C1 INT, C2 INT)");
    execute_statement("CREATE INDEX I ON T(C1)");
    execute_statement("INSERT INTO T VALUES (1, 1, 1)");
    execute_statement("INSERT OR REPLACE INTO T VALUES (1, 10, 100)");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT C0, C1, C2 FROM T WHERE C1 = 10 ORDER BY C0", result);
    ASSERT_EQ(1, result.size());
    ASSERT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 10, 100)), result[0]);
    {
        std::string plan{};
        explain_statement("SELECT C0, C1, C2 FROM T WHERE C1 = 10 ORDER BY C0", plan);
        // verify 1. find op exists 2. with target index I
        EXPECT_TRUE(contains(plan,
            R"({"kind":"find","this":"@2","source":{"kind":"relation","binding":{"kind":"index","table":"T","simple_name":"I")"
        ));
    }
}

TEST_F(secondary_index_upsert_test, upsert_updates_existing_entry_on_secondary_index_no_update_on_index_key) {
    // INSERT OR REPLACE replaces existing entry but no update for index key (just upsert on secondary index)
    execute_statement("CREATE TABLE T(C0 INT PRIMARY KEY, C1 INT, C2 INT)");
    execute_statement("CREATE INDEX I ON T(C1)");
    execute_statement("INSERT INTO T VALUES (1, 10, 1)");
    execute_statement("INSERT OR REPLACE INTO T VALUES (1, 10, 100)");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT C0, C1, C2 FROM T WHERE C1 = 10 ORDER BY C0", result);
    ASSERT_EQ(1, result.size());
    ASSERT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 10, 100)), result[0]);
    {
        std::string plan{};
        explain_statement("SELECT C0, C1, C2 FROM T WHERE C1 = 10 ORDER BY C0", plan);
        // verify 1. find op exists 2. with target index I
        EXPECT_TRUE(contains(plan,
            R"({"kind":"find","this":"@2","source":{"kind":"relation","binding":{"kind":"index","table":"T","simple_name":"I")"
        ));
    }
}
}
