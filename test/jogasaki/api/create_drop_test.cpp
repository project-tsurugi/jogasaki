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

#include <memory>
#include <string>
#include <string_view>
#include <vector>
#include <gtest/gtest.h>

#include <takatori/datetime/date.h>
#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/time_point.h>
#include <takatori/decimal/triple.h>
#include <takatori/util/downcast.h>

#include <jogasaki/api/impl/database.h>
#include <jogasaki/configuration.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/port.h>
#include <jogasaki/recovery/storage_options.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/storage/storage_manager.h>
#include <jogasaki/utils/create_tx.h>
#include <jogasaki/utils/get_storage_by_index_name.h>
#include <jogasaki/utils/surrogate_id_utils.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
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

/**
 * @brief testcases for storage manager entries modified by create/drop DDLs
 */
class create_drop_test:
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

bool create_drop_test::has_storage_key(kvs::storage& s) {
    sharksfin::StorageOptions options{};
    EXPECT_EQ(status::ok, s.get_options(options));

    proto::metadata::storage::IndexDefinition idef{};
    std::uint64_t message_version{};
    EXPECT_TRUE(! recovery::validate_extract(options.payload(), idef, message_version));
    return proto::metadata::storage::IndexDefinition::StorageKeyOptionalCase::STORAGE_KEY_OPTIONAL_NOT_SET != idef.storage_key_optional_case();
}

using namespace std::string_view_literals;

TEST_F(create_drop_test, create0) {
    utils::set_global_tx_option({false, true}); // to customize
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY)");
    execute_statement("INSERT INTO T (C0) VALUES(1)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T", result);
    }
    auto& smgr = *global::storage_manager();
    auto e = smgr.find_by_name("T");
    ASSERT_TRUE(e.has_value());
    auto s = smgr.find_entry(e.value());
    ASSERT_TRUE(s);
}

TEST_F(create_drop_test, drop0) {
    utils::set_global_tx_option({true, false}); // to customize
    execute_statement("CREATE TABLE TT (C0 INT NOT NULL PRIMARY KEY)");
    execute_statement("INSERT INTO TT (C0) VALUES(1)");
    auto& smgr = *global::storage_manager();
    auto e = smgr.find_by_name("TT");
    ASSERT_TRUE(e.has_value());
    auto s = smgr.find_entry(e.value());
    ASSERT_TRUE(s);
    execute_statement("DROP TABLE TT");
    ASSERT_TRUE(! smgr.find_by_name("TT").has_value());
    ASSERT_TRUE(! smgr.find_entry(e.value()));
    execute_statement("CREATE TABLE TT2 (C0 INT NOT NULL PRIMARY KEY)");
    auto e2 = smgr.find_by_name("TT2");
    ASSERT_TRUE(e2.has_value());
    ASSERT_TRUE(! smgr.find_entry(e.value())); // TT2 id must be different from TT id, should not be recycled
    execute_statement("INSERT INTO TT2 (C0) VALUES(1)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM TT2", result);
        ASSERT_EQ(1, result.size());
    }
}

TEST_F(create_drop_test, verify_storage_key_for_tables) {
    execute_statement("CREATE TABLE t0 (C0 BIGINT NOT NULL PRIMARY KEY, C1 DOUBLE)");
    execute_statement("INSERT INTO t0 (C0, C1) VALUES(1,1.0)");
    std::uint64_t v0{}, v1{};
    std::optional<std::string> sk0{}, sk1{};
    {
        auto provider = db_impl()->tables();
        auto i0 = provider->find_index("t0");
        ASSERT_TRUE(i0);
        auto s0 = utils::get_storage_by_index_name("t0");
        ASSERT_TRUE(s0);
        EXPECT_TRUE(has_storage_key(*s0));
        auto e = global::storage_manager()->find_by_name("t0");
        ASSERT_TRUE(e.has_value());
        auto cb = global::storage_manager()->find_entry(e.value());
        ASSERT_TRUE(cb);
        sk0 = global::storage_manager()->get_storage_key("t0");
        ASSERT_TRUE(sk0);
        v0 = utils::from_big_endian(sk0.value());
        EXPECT_GT(v0, 0) << v0;
    }
    execute_statement("CREATE TABLE t1 (C0 BIGINT NOT NULL PRIMARY KEY, C1 DOUBLE)");
    {
        auto provider = db_impl()->tables();
        auto i1 = provider->find_index("t1");
        ASSERT_TRUE(i1);
        auto s1 = utils::get_storage_by_index_name("t1");
        ASSERT_TRUE(s1);
        EXPECT_TRUE(has_storage_key(*s1));
        auto e = global::storage_manager()->find_by_name("t1");
        ASSERT_TRUE(e.has_value());
        auto cb = global::storage_manager()->find_entry(e.value());
        ASSERT_TRUE(cb);
        sk1 = global::storage_manager()->get_storage_key("t1");
        ASSERT_TRUE(sk1);
        v1 = utils::from_big_endian(sk1.value());
        EXPECT_GT(v1, 0) << v1;
    }
    EXPECT_LT(v0, v1);
    std::cerr << "******************* v0:" << v0 << " v1:" << v1 << std::endl;
    execute_statement("DROP TABLE t0");
    {
        auto provider = db_impl()->tables();
        auto i0 = provider->find_index("t0");
        ASSERT_TRUE(! i0);
        auto s0 = utils::get_storage_by_index_name("t0");
        ASSERT_TRUE(! s0);
        auto e = global::storage_manager()->find_by_name("t0");
        ASSERT_TRUE(! e.has_value());
        auto sk = global::storage_manager()->get_storage_key("t0");
        ASSERT_TRUE(! sk);
        auto n = global::storage_manager()->get_index_name(sk0.value());
        ASSERT_TRUE(! n);
    }
}

TEST_F(create_drop_test, verify_storage_key_for_indices) {
    execute_statement("CREATE TABLE t0 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("CREATE INDEX i0 ON t0(c1)");
    std::uint64_t v0{}, v1{};
    std::optional<std::string> sk0{}, sk1{};
    {
        auto provider = db_impl()->tables();
        auto i0 = provider->find_index("i0");
        EXPECT_TRUE(i0);
        auto s0 = utils::get_storage_by_index_name("i0");
        ASSERT_TRUE(s0);
        EXPECT_TRUE(has_storage_key(*s0));
        auto e = global::storage_manager()->find_by_name("i0");
        ASSERT_TRUE(e.has_value());
        auto cb = global::storage_manager()->find_entry(e.value());
        EXPECT_TRUE(cb);
        sk0 = global::storage_manager()->get_storage_key("i0");
        ASSERT_TRUE(sk0);
        v0 = utils::from_big_endian(sk0.value());
        EXPECT_GT(v0, 0) << v0;
        auto n = global::storage_manager()->get_index_name(sk0.value());
        ASSERT_TRUE(n);
        EXPECT_EQ("i0", n.value());
    }
    execute_statement("CREATE INDEX i1 ON t0 (c1)");
    {
        auto provider = db_impl()->tables();
        auto i1 = provider->find_index("i1");
        EXPECT_TRUE(i1);
        auto s1 = utils::get_storage_by_index_name("i1");
        ASSERT_TRUE(s1);
        EXPECT_TRUE(has_storage_key(*s1));
        auto e = global::storage_manager()->find_by_name("i1");
        ASSERT_TRUE(e.has_value());
        auto cb = global::storage_manager()->find_entry(e.value());
        EXPECT_TRUE(cb);
        sk1 = global::storage_manager()->get_storage_key("i1");
        ASSERT_TRUE(sk1);
        v1 = utils::from_big_endian(sk1.value());
        EXPECT_GT(v1, 0) << v1;
        auto n = global::storage_manager()->get_index_name(sk1.value());
        ASSERT_TRUE(n);
        EXPECT_EQ("i1", n.value());
    }
    EXPECT_LT(v0, v1);
    std::cerr << "******************* v0:" << v0 << " v1:" << v1 << std::endl;

    // verify indices dropped explicitly
    execute_statement("DROP INDEX i0");
    {
        auto provider = db_impl()->tables();
        auto i = provider->find_index("i0");
        EXPECT_TRUE(! i);
        auto s = utils::get_storage_by_index_name("i0");
        EXPECT_TRUE(! s);
        auto sk = global::storage_manager()->get_storage_key("i0");
        EXPECT_TRUE(! sk);
        auto n = global::storage_manager()->get_index_name(sk0.value());
        EXPECT_TRUE(! n);
    }

    // verify indices cascade-dropped
    execute_statement("DROP TABLE t0");
    {
        auto provider = db_impl()->tables();
        auto i = provider->find_index("i1");
        EXPECT_TRUE(! i);
        auto s = utils::get_storage_by_index_name("i1");
        EXPECT_TRUE(! s);
        auto sk = global::storage_manager()->get_storage_key("i1");
        EXPECT_TRUE(! sk);
        auto n = global::storage_manager()->get_index_name(sk1.value());
        EXPECT_TRUE(! n);
    }
}

TEST_F(create_drop_test, system_table_has_no_storage_key) {
    // verify system table (__system_sequences) has no `storage_key` field and remain same as existing tables
    auto& smgr = *global::storage_manager();
    auto e = smgr.find_by_name(system_sequences_name);
    ASSERT_TRUE(e.has_value());
    auto cb = smgr.find_entry(e.value());
    ASSERT_TRUE(cb);
    EXPECT_TRUE(! cb->storage_key().has_value());

    auto s = global::db()->get_storage(system_sequences_name);
    ASSERT_TRUE(s);
    EXPECT_TRUE(! has_storage_key(*s));
}

TEST_F(create_drop_test, tables_with_no_storage_key) {
    // simulate pre-1.8 indices (no `storage_key` field)
    global::config_pool()->enable_storage_key(false);
    execute_statement("CREATE TABLE t0 (c0 int primary key, c1 int)");
    global::config_pool()->enable_storage_key(true);
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
TEST_F(create_drop_test, index_with_no_storage_key) {
    // simulate pre-1.8 indices (no `storage_key` field)
    global::config_pool()->enable_storage_key(false);
    execute_statement("CREATE TABLE t0 (c0 int primary key, c1 int)");
    execute_statement("CREATE INDEX i0 on t0(c1)");
    global::config_pool()->enable_storage_key(true);
    {
        auto provider = db_impl()->tables();
        auto i0 = provider->find_index("i0");
        EXPECT_TRUE(i0);
        auto s0 = utils::get_storage_by_index_name("i0");
        EXPECT_TRUE(s0);
        auto s = global::db()->get_storage("i0");
        ASSERT_TRUE(s);
        EXPECT_TRUE(! has_storage_key(*s));
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

TEST_F(create_drop_test, tables_with_no_storage_key_grant_revoke) {
    // verify once table is created with no storage_key, grant/revoke won't add one
    global::config_pool()->enable_storage_key(false);
    execute_statement("CREATE TABLE t0 (c0 int primary key, c1 int)");
    global::config_pool()->enable_storage_key(true);
    execute_statement("grant select, insert on table t0 to user1");
    execute_statement("revoke insert on table t0 from user1");

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
