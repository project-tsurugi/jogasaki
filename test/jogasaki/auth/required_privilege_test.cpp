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

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include <takatori/type/primitive.h>

#include <yugawara/aggregate/configurable_provider.h>
#include <yugawara/storage/basic_configurable_provider.h>
#include <yugawara/storage/column.h>
#include <yugawara/storage/configurable_provider.h>
#include <yugawara/storage/index.h>
#include <yugawara/storage/index_feature.h>
#include <yugawara/storage/relation.h>
#include <yugawara/storage/table.h>
#include <yugawara/variable/criteria.h>
#include <yugawara/variable/nullity.h>
#include <yugawara/variable/criteria.h>
#include <yugawara/variable/nullity.h>

#include <jogasaki/auth/action_kind.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/plan/compiler.h>
#include <jogasaki/plan/compiler_context.h>
#include <jogasaki/plan/mirror_container.h>
#include <jogasaki/storage/storage_manager.h>

namespace jogasaki::auth {

namespace type = ::takatori::type;

class required_privilege_test : public ::testing::Test {
public:
    static void get_action_set_for_sql(
        std::string_view sql,
        jogasaki::auth::action_set& out_actions,
        std::string_view table_name = "t"
    ) {
        auto mgr = jogasaki::global::storage_manager();
        ASSERT_TRUE(static_cast<bool>(mgr));
        mgr->clear();
        (void) mgr->add_entry(1, table_name);

        jogasaki::plan::compiler_context ctx{};
        ctx.storage_provider(make_tables());
        ctx.function_provider(jogasaki::global::scalar_function_provider());
        ctx.aggregate_provider(std::make_shared<::yugawara::aggregate::configurable_provider>());

        ASSERT_EQ(jogasaki::status::ok, jogasaki::plan::prepare(sql, ctx));
        auto const& p = ctx.prepared_statement();
        ASSERT_TRUE(static_cast<bool>(p));
        ASSERT_TRUE(static_cast<bool>(p->mirrors()));

        auto& op = p->mirrors()->mutable_storage_operation();
        for (auto&& [entry, actions] : op) {
            if(entry != 1) {
                continue;
            }
            (void) entry;
            out_actions = actions;
            return;
        }
        throw std::runtime_error("No action_set found");
    }
protected:
    static std::shared_ptr<::yugawara::storage::configurable_provider> make_tables() {
        auto provider = std::make_shared<::yugawara::storage::configurable_provider>();
        add_table(*provider, "t");
        add_table(*provider, "s");
        add_table_compound_pkey(*provider, "u");
        return provider;
    }
    static void add_table(::yugawara::storage::configurable_provider& provider, std::string_view table_name) {
        auto t = provider.add_table({
            table_name,
            {
                {"c0", type::int8(), ::yugawara::variable::criteria{::yugawara::variable::nullity{false}}},
                {"c1", type::int8()},
            },
        });
        // primary index to enable scan
        (void) provider.add_index({
            t,
            table_name,
            { t->columns()[0] },
            { t->columns()[1] },
            {
                ::yugawara::storage::index_feature::find,
                ::yugawara::storage::index_feature::scan,
                ::yugawara::storage::index_feature::unique,
                ::yugawara::storage::index_feature::primary,
            },
        });
    }
    static void add_table_compound_pkey(::yugawara::storage::configurable_provider& provider, std::string_view table_name) {
        auto t = provider.add_table({
            table_name,
            {
                {"c0", type::int8(), ::yugawara::variable::criteria{::yugawara::variable::nullity{false}}},
                {"c1", type::int8(), ::yugawara::variable::criteria{::yugawara::variable::nullity{false}}},
                {"c2", type::int8()},
            },
        });
        // primary index to enable scan
        (void) provider.add_index({
            t,
            table_name,
            {
                t->columns()[0],
                t->columns()[1],
            },
            { t->columns()[2] },
            {
                ::yugawara::storage::index_feature::find,
                ::yugawara::storage::index_feature::scan,
                ::yugawara::storage::index_feature::unique,
                ::yugawara::storage::index_feature::primary,
            },
        });
    }
};

TEST_F(required_privilege_test, scan) {
    // scan op requires select privilege
    jogasaki::auth::action_set actions{};
    get_action_set_for_sql("SELECT * FROM t", actions);
    EXPECT_TRUE(actions.has_action(action_kind::select));
    EXPECT_TRUE(actions.action_allowed(action_kind::select));
}

TEST_F(required_privilege_test, find) {
    // find op requires select privilege
    jogasaki::auth::action_set actions{};
    get_action_set_for_sql("SELECT * FROM t WHERE c0=1", actions);
    EXPECT_TRUE(actions.has_action(action_kind::select));
    EXPECT_TRUE(actions.action_allowed(action_kind::select));
}

TEST_F(required_privilege_test, join_find) {
    // join_find op requires select privilege
    jogasaki::auth::action_set actions{};
    get_action_set_for_sql("SELECT * FROM t t0 join t t1 on t0.c1=t1.c0", actions);
    EXPECT_TRUE(actions.has_action(action_kind::select));
    EXPECT_TRUE(actions.action_allowed(action_kind::select));
}

TEST_F(required_privilege_test, join_scan) {
    // join_find op requires select privilege
    jogasaki::auth::action_set actions{};
    get_action_set_for_sql("SELECT * FROM t join u on t.c1=u.c0", actions);
    EXPECT_TRUE(actions.has_action(action_kind::select));
    EXPECT_TRUE(actions.action_allowed(action_kind::select));
}

TEST_F(required_privilege_test, insert) {
    // simple insert requires insert privilege
    jogasaki::auth::action_set actions{};
    get_action_set_for_sql("INSERT INTO t VALUES(1, 1)", actions);
    EXPECT_TRUE(actions.has_action(action_kind::insert));
    EXPECT_TRUE(actions.action_allowed(action_kind::insert));
}

TEST_F(required_privilege_test, insert_or_ignore) {
    // insert or ignore requires insert privilege
    jogasaki::auth::action_set actions{};
    get_action_set_for_sql("INSERT OR IGNORE INTO t VALUES(1, 1)", actions);
    EXPECT_TRUE(actions.has_action(action_kind::insert));
    EXPECT_TRUE(actions.action_allowed(action_kind::insert));
}

TEST_F(required_privilege_test, insert_or_replace) {
    // insert or replace requires insert and update privileges
    jogasaki::auth::action_set actions{};
    get_action_set_for_sql("INSERT OR REPLACE INTO t VALUES(1, 1)", actions);
    EXPECT_TRUE(actions.has_action(action_kind::insert));
    EXPECT_TRUE(actions.action_allowed(action_kind::insert));
    EXPECT_TRUE(actions.has_action(action_kind::update));
    EXPECT_TRUE(actions.action_allowed(action_kind::update));
}

TEST_F(required_privilege_test, delete) {
    // delete requires select and delete privileges
    jogasaki::auth::action_set actions{};
    get_action_set_for_sql("DELETE FROM t", actions);
    EXPECT_TRUE(actions.has_action(action_kind::select));
    EXPECT_TRUE(actions.action_allowed(action_kind::select));
    EXPECT_TRUE(actions.has_action(action_kind::delete_));
    EXPECT_TRUE(actions.action_allowed(action_kind::delete_));
}

TEST_F(required_privilege_test, update) {
    // update requires select and update privileges
    jogasaki::auth::action_set actions{};
    get_action_set_for_sql("UPDATE t SET c1=1", actions);
    EXPECT_TRUE(actions.has_action(action_kind::select));
    EXPECT_TRUE(actions.action_allowed(action_kind::select));
    EXPECT_TRUE(actions.has_action(action_kind::update));
    EXPECT_TRUE(actions.action_allowed(action_kind::update));
}

TEST_F(required_privilege_test, insert_select_same_table) {
    // insert with select requires both select and insert privileges
    jogasaki::auth::action_set actions{};
    get_action_set_for_sql("INSERT INTO t SELECT * FROM t", actions);
    EXPECT_TRUE(actions.has_action(action_kind::select));
    EXPECT_TRUE(actions.action_allowed(action_kind::select));
    EXPECT_TRUE(actions.has_action(action_kind::insert));
    EXPECT_TRUE(actions.action_allowed(action_kind::insert));
}

TEST_F(required_privilege_test, insert_select_different_table) {
    // insert with select requires select and insert privileges for each table respectively
    {
        jogasaki::auth::action_set actions{};
        get_action_set_for_sql("INSERT INTO s SELECT * FROM t", actions, "t");
        EXPECT_TRUE(actions.has_action(action_kind::select));
        EXPECT_TRUE(actions.action_allowed(action_kind::select));
        EXPECT_TRUE(! actions.has_action(action_kind::insert));
        EXPECT_TRUE(! actions.action_allowed(action_kind::insert));
    }
    {
        jogasaki::auth::action_set actions{};
        get_action_set_for_sql("INSERT INTO s SELECT * FROM t", actions, "s");
        EXPECT_TRUE(! actions.has_action(action_kind::select));
        EXPECT_TRUE(! actions.action_allowed(action_kind::select));
        EXPECT_TRUE(actions.has_action(action_kind::insert));
        EXPECT_TRUE(actions.action_allowed(action_kind::insert));
    }
}

} // namespace jogasaki::auth
