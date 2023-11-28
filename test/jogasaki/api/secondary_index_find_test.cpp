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

class secondary_index_find_test :
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

// TODO secondary key is not used for range scan for now, so testing ORDER BY with secondary key is not here. Add when it's implemented.

TEST_F(secondary_index_find_test, find_by_asc_secondary_key) {
    execute_statement("INSERT INTO TEST (C0, K1, K2, V1) VALUES (3, 3, 3, 3)");
    execute_statement("INSERT INTO TEST (C0) VALUES (2)");
    execute_statement("INSERT INTO TEST (C0, K1, K2, V1) VALUES (0, 0, 0, 0)");
    execute_statement("INSERT INTO TEST (C0, K1, K2, V1) VALUES (1, 0, 0, 0)");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT C0 FROM TEST WHERE K1 = 0 ORDER BY C0", result);
    ASSERT_EQ(2, result.size());
    ASSERT_EQ((mock::create_nullable_record<kind::int8>(0)), result[0]);
    ASSERT_EQ((mock::create_nullable_record<kind::int8>(1)), result[1]);
}

TEST_F(secondary_index_find_test, find_by_desc_secondary_key) {
    execute_statement("INSERT INTO TEST (C0, K1, K2, V1) VALUES (3, 3, 3, 3)");
    execute_statement("INSERT INTO TEST (C0) VALUES (2)");
    execute_statement("INSERT INTO TEST (C0, K1, K2, V1) VALUES (0, 0, 0, 0)");
    execute_statement("INSERT INTO TEST (C0, K1, K2, V1) VALUES (1, 0, 0, 0)");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT C0 FROM TEST WHERE K2 = 0 ORDER BY C0", result);
    ASSERT_EQ(2, result.size());
    ASSERT_EQ((mock::create_nullable_record<kind::int8>(0)), result[0]);
    ASSERT_EQ((mock::create_nullable_record<kind::int8>(1)), result[1]);
}
}
