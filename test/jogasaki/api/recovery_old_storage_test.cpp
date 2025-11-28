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
#include <chrono>
#include <cstddef>
#include <initializer_list>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <takatori/type/primitive.h>
#include <takatori/type/type_kind.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <yugawara/storage/basic_configurable_provider.h>
#include <yugawara/storage/column.h>
#include <yugawara/storage/index.h>
#include <yugawara/storage/index_feature.h>
#include <yugawara/storage/relation_kind.h>
#include <yugawara/storage/sequence.h>
#include <yugawara/storage/table.h>
#include <yugawara/variable/nullity.h>

#include <jogasaki/api/impl/database.h>
#include <jogasaki/configuration.h>
#include <jogasaki/constants.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/sequence/manager.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/id.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/recovery/storage_options.h>
#include <jogasaki/status.h>
#include <jogasaki/storage/storage_manager.h>
#include <jogasaki/utils/add_test_tables.h>
#include <jogasaki/utils/create_tx.h>
#include <jogasaki/utils/get_storage_by_index_name.h>
#include <jogasaki/utils/storage_dump_formatter.h>
#include <jogasaki/utils/tables.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace std::chrono_literals;

using namespace yugawara;
using namespace yugawara::storage;
using index = yugawara::storage::index;

namespace type = takatori::type;
using nullity = yugawara::variable::nullity;

using kind = jogasaki::meta::field_type_kind;
/**
 * @brief test database recovery with pre-1.8 indices (no `storage_key`)
 */
class recovery_old_storage_test :
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
    bool has_storage_key(kvs::storage& s);
};

// copied from create_drop_test
bool recovery_old_storage_test::has_storage_key(kvs::storage& s) {
    sharksfin::StorageOptions options{};
    EXPECT_EQ(status::ok, s.get_options(options));

    proto::metadata::storage::IndexDefinition idef{};
    std::uint64_t message_version{};
    EXPECT_TRUE(! recovery::validate_extract(options.payload(), idef, message_version));
    return proto::metadata::storage::IndexDefinition::StorageKeyOptionalCase::STORAGE_KEY_OPTIONAL_NOT_SET != idef.storage_key_optional_case();
}

TEST_F(recovery_old_storage_test, recover_old_table) {
    // recover old table and verify the status
    utils::set_global_tx_option({false, false});  // to customize scenario
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory doesn't support recovery";
    }
    // simulate pre-1.8 indices (no `storage_key` field)
    global::config_pool()->enable_storage_key(false);
    execute_statement("CREATE TABLE t0 (c0 int primary key, c1 int)");
    global::config_pool()->enable_storage_key(true);

    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());

    {
        auto provider = db_impl()->tables();
        auto i0 = provider->find_index("t0");
        EXPECT_TRUE(i0);
        auto s0 = utils::get_storage_by_index_name("t0");
        ASSERT_TRUE(s0);
        EXPECT_TRUE(! has_storage_key(*s0));
        auto s = global::db()->get_storage("t0");
        EXPECT_TRUE(s);
        auto e = global::storage_manager()->find_by_name("t0");
        ASSERT_TRUE(e.has_value());
        auto cb = global::storage_manager()->find_entry(e.value());
        ASSERT_TRUE(cb);
        EXPECT_TRUE(! cb->storage_key().has_value());
        auto sk0 = global::storage_manager()->get_storage_key("t0");
        ASSERT_TRUE(sk0);
        EXPECT_EQ("t0", sk0.value());
        auto n = global::storage_manager()->get_index_name("t0");
        ASSERT_TRUE(n);
        EXPECT_EQ("t0", n.value());
    }
    execute_statement("DROP TABLE t0");
    {
        auto provider = db_impl()->tables();
        auto i0 = provider->find_index("t0");
        EXPECT_TRUE(! i0);
        auto s0 = utils::get_storage_by_index_name("t0");
        EXPECT_TRUE(! s0);
        auto s = global::db()->get_storage("t0");
        EXPECT_TRUE(! s);
        auto e = global::storage_manager()->find_by_name("t0");
        ASSERT_TRUE(! e.has_value());
        auto sk0 = global::storage_manager()->get_storage_key("t0");
        ASSERT_TRUE(! sk0);
        auto n = global::storage_manager()->get_index_name("t0");
        ASSERT_TRUE(! n);
    }
}

TEST_F(recovery_old_storage_test, recover_old_index) {
    // recover old index and verify the status
    utils::set_global_tx_option({false, false});  // to customize scenario
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory doesn't support recovery";
    }
    // simulate pre-1.8 indices (no `storage_key` field)
    global::config_pool()->enable_storage_key(false);
    execute_statement("CREATE TABLE t0 (c0 int primary key, c1 int)");
    execute_statement("CREATE INDEX i0 on t0(c1)");
    global::config_pool()->enable_storage_key(true);

    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());

    {
        auto provider = db_impl()->tables();
        auto i0 = provider->find_index("i0");
        EXPECT_TRUE(i0);
        auto s0 = utils::get_storage_by_index_name("i0");
        ASSERT_TRUE(s0);
        EXPECT_TRUE(! has_storage_key(*s0));
        auto s = global::db()->get_storage("i0");
        EXPECT_TRUE(s);
        auto e = global::storage_manager()->find_by_name("i0");
        ASSERT_TRUE(e.has_value());
        auto cb = global::storage_manager()->find_entry(e.value());
        ASSERT_TRUE(cb);
        EXPECT_TRUE(! cb->storage_key().has_value());
        auto sk0 = global::storage_manager()->get_storage_key("i0");
        ASSERT_TRUE(sk0);
        EXPECT_EQ("i0", sk0.value());
        auto n = global::storage_manager()->get_index_name("i0");
        ASSERT_TRUE(n);
        EXPECT_EQ("i0", n.value());
    }
    execute_statement("DROP INDEX i0");
    {
        auto provider = db_impl()->tables();
        auto i0 = provider->find_index("i0");
        EXPECT_TRUE(! i0);
        auto s0 = utils::get_storage_by_index_name("i0");
        EXPECT_TRUE(! s0);
        auto s = global::db()->get_storage("i0");
        EXPECT_TRUE(! s);
        auto e = global::storage_manager()->find_by_name("i0");
        ASSERT_TRUE(! e.has_value());
        auto sk0 = global::storage_manager()->get_storage_key("i0");
        ASSERT_TRUE(! sk0);
        auto n = global::storage_manager()->get_index_name("i0");
        ASSERT_TRUE(! n);
    }
}

TEST_F(recovery_old_storage_test, tables_with_no_storage_key_grant_revoke) {
    // verify once table is created with no storage_key, grant/revoke with recovery won't add one
    global::config_pool()->enable_storage_key(false);
    execute_statement("CREATE TABLE t0 (c0 int primary key, c1 int)");
    global::config_pool()->enable_storage_key(true);

    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());

    execute_statement("grant select, insert on table t0 to user1");
    execute_statement("revoke insert on table t0 from user1");

    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());
    {
        auto provider = db_impl()->tables();
        auto i0 = provider->find_index("t0");
        EXPECT_TRUE(i0);
        auto s0 = utils::get_storage_by_index_name("t0");
        EXPECT_TRUE(s0);
        auto s = global::db()->get_storage("t0");
        ASSERT_TRUE(s);
        EXPECT_TRUE(! has_storage_key(*s));
        auto e = global::storage_manager()->find_by_name("t0");
        ASSERT_TRUE(e.has_value());
        auto cb = global::storage_manager()->find_entry(e.value());
        ASSERT_TRUE(cb);
        EXPECT_TRUE(! cb->storage_key().has_value());
        auto sk0 = global::storage_manager()->get_storage_key("t0");
        ASSERT_TRUE(sk0);
        EXPECT_EQ("t0", sk0.value());
        auto n = global::storage_manager()->get_index_name("t0");
        ASSERT_TRUE(n);
        EXPECT_EQ("t0", n.value());
    }
}

}
