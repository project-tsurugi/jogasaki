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

#include <memory>
#include <string>
#include <string_view>
#include <vector>
#include <boost/move/utility_core.hpp>
#include <gtest/gtest.h>

#include <takatori/type/type_kind.h>
#include <takatori/util/downcast.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/value/value_kind.h>
#include <yugawara/storage/basic_configurable_provider.h>
#include <yugawara/storage/index.h>
#include <yugawara/storage/index_feature.h>
#include <yugawara/storage/sequence.h>
#include <yugawara/storage/table.h>
#include <yugawara/variable/nullity.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/configuration.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/port.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/test_utils/secondary_index.h>

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
using api::impl::get_impl;
using kvs::end_point_kind;

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
    {
        auto entries = utils::get_secondary_entries(
            *get_impl(*db_).kvs_db(),
            *get_impl(*db_).tables()->find_index("T"),
            *get_impl(*db_).tables()->find_index("I"),
            mock::create_nullable_record<kind::int4>(),
            mock::create_nullable_record<kind::int4>());
        ASSERT_EQ(1, entries.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int4>(10)), entries[0].first);
        EXPECT_EQ((mock::create_nullable_record<kind::int4>(1)), entries[0].second);
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
    {
        auto entries = utils::get_secondary_entries(
            *get_impl(*db_).kvs_db(),
            *get_impl(*db_).tables()->find_index("T"),
            *get_impl(*db_).tables()->find_index("I"),
            mock::create_nullable_record<kind::int4>(),
            mock::create_nullable_record<kind::int4>());
        ASSERT_EQ(1, entries.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int4>(10)), entries[0].first);
        EXPECT_EQ((mock::create_nullable_record<kind::int4>(1)), entries[0].second);
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
    {
        auto entries = utils::get_secondary_entries(
            *get_impl(*db_).kvs_db(),
            *get_impl(*db_).tables()->find_index("T"),
            *get_impl(*db_).tables()->find_index("I"),
            mock::create_nullable_record<kind::int4>(),
            mock::create_nullable_record<kind::int4>());
        ASSERT_EQ(1, entries.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int4>(10)), entries[0].first);
        EXPECT_EQ((mock::create_nullable_record<kind::int4>(1)), entries[0].second);
    }
}

}
