/*
 * Copyright 2018-2026 Project Tsurugi.
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
#include <algorithm>
#include <array>
#include <atomic>
#include <memory>
#include <limits>
#include <set>
#include <thread>
#include <vector>
#include <gtest/gtest.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/id.h>
#include <jogasaki/kvs/transaction.h>
#include <jogasaki/storage/definition_id_allocator.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/test_utils.h>
#include <jogasaki/utils/create_tx.h>
#include <jogasaki/proto/metadata/storage.pb.h>
#include <jogasaki/storage/storage_manager.h>
#include "api_test_base.h"

namespace jogasaki::testing {
class definition_id_test : public ::testing::Test, public api_test_base {
protected:
    bool to_explain() override { return false; }
    void SetUp() override { db_setup(std::make_shared<configuration>()); }
    void TearDown() override { db_teardown(); }
    std::array<std::uint64_t, 2> ids(std::string_view name) {
        auto index = db_impl()->tables()->find_index(name);
        EXPECT_TRUE(index);
        if (!index || !index->definition_id() || !index->table().definition_id()) {
            ADD_FAILURE() << "missing definition identity";
            return {};
        }
        return {*index->table().definition_id(), *index->definition_id()};
    }
};

TEST_F(definition_id_test, normal_tables_and_secondary_indices_receive_ids) {
    execute_statement("CREATE TABLE A(K BIGINT PRIMARY KEY, V VARCHAR(100))");
    execute_statement("CREATE TABLE B(K INT PRIMARY KEY, V DATE)");
    execute_statement("CREATE INDEX A_V ON A(V)");
    auto a = ids("A"), b = ids("B");
    auto secondary = db_impl()->tables()->find_index("A_V");
    ASSERT_TRUE(secondary);
    ASSERT_TRUE(secondary->definition_id());
    EXPECT_EQ(a[0], secondary->table().definition_id());
    EXPECT_EQ(5, (std::set<std::uint64_t>{a[0], a[1], b[0], b[1], *secondary->definition_id()}).size());
}

TEST_F(definition_id_test, reusable_prepared_create_never_reuses_definition_identity) {
    api::statement_handle prepared{};
    ASSERT_EQ(status::ok, db_->prepare("CREATE TABLE T(K BIGINT PRIMARY KEY)", prepared));
    execute_statement(prepared);
    auto first = ids("T");
    execute_statement("DROP TABLE T");
    execute_statement(prepared);
    auto second = ids("T");
    EXPECT_GT(second[0], first[1]);
    EXPECT_GT(second[1], second[0]);
    ASSERT_EQ(status::ok, db_->destroy_statement(prepared));
}

TEST_F(definition_id_test, ids_and_secondary_table_identity_survive_restart) {
    if (kvs::implementation_id() == "memory") GTEST_SKIP();
    execute_statement("CREATE TABLE T(K BIGINT PRIMARY KEY, V BIGINT)");
    execute_statement("CREATE INDEX T_V ON T(V)");
    auto first = ids("T");
    auto secondary = db_impl()->tables()->find_index("T_V")->definition_id();
    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());
    EXPECT_EQ(first, ids("T"));
    EXPECT_EQ(secondary, db_impl()->tables()->find_index("T_V")->definition_id());
    EXPECT_EQ(first[0], db_impl()->tables()->find_index("T_V")->table().definition_id());
    EXPECT_EQ(db_impl()->tables()->find_table("T").get(), std::addressof(db_impl()->tables()->find_index("T_V")->table()));
}

TEST_F(definition_id_test, dropped_maximum_and_restart_do_not_rewind_allocator) {
    if (kvs::implementation_id() == "memory") GTEST_SKIP();
    execute_statement("CREATE TABLE T(K BIGINT PRIMARY KEY)");
    auto first = ids("T");
    execute_statement("DROP TABLE T");
    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());
    execute_statement("CREATE TABLE T(K BIGINT PRIMARY KEY)");
    EXPECT_GT(ids("T")[0], first[1]);
}

TEST_F(definition_id_test, caller_rollback_does_not_undo_an_id_reservation) {
    auto tx = global::db()->create_transaction();
    ASSERT_TRUE(tx);
    std::uint64_t first{}, next{};
    ASSERT_EQ(status::ok, db_impl()->definition_ids()->reserve(2, 1, first));
    ASSERT_EQ(status::ok, tx->abort_transaction());
    ASSERT_EQ(status::ok, db_impl()->definition_ids()->reserve(1, 1, next));
    EXPECT_EQ(first + 2, next);
}

TEST_F(definition_id_test, parallel_reservations_are_disjoint) {
    constexpr std::size_t threads = 4, batches = 8;
    std::array<std::array<std::uint64_t, batches>, threads> values{};
    std::atomic_bool failed{false};
    std::vector<std::thread> workers{};
    for (std::size_t t=0; t<threads; ++t) workers.emplace_back([&, t] {
        for (std::size_t n=0; n<batches; ++n) {
            if (db_impl()->definition_ids()->reserve(2, 1, values[t][n]) != status::ok) failed = true;
        }
    });
    for (auto& worker : workers) worker.join();
    ASSERT_FALSE(failed);
    std::set<std::uint64_t> ids{};
    for (auto const& row : values) for (auto first : row) { EXPECT_TRUE(ids.insert(first).second); EXPECT_TRUE(ids.insert(first + 1).second); }
    EXPECT_EQ(threads * batches * 2, ids.size());
}
TEST_F(definition_id_test, sql_rollback_preserves_existing_ddl_contract_and_does_not_reuse_ids) {
    auto tx = utils::create_transaction(*db_);
    execute_statement("CREATE TABLE T(K BIGINT PRIMARY KEY)", *tx);
    auto before = ids("T");
    ASSERT_EQ(status::ok, tx->abort());
    // Existing Jogasaki DDL creates the definition independently of SQL transaction rollback.
    EXPECT_EQ(before, ids("T"));
    execute_statement("DROP TABLE T");
    execute_statement("CREATE TABLE T(K BIGINT PRIMARY KEY)");
    EXPECT_GT(ids("T")[0], before[1]);
}

TEST_F(definition_id_test, parallel_ddl_uses_unique_ids) {
    constexpr std::size_t count = 4;
    std::vector<std::thread> workers{};
    for (std::size_t i=0; i<count; ++i) workers.emplace_back([&, i] {
        execute_statement("CREATE TABLE P" + std::to_string(i) + "(K BIGINT PRIMARY KEY)");
    });
    for (auto& worker : workers) worker.join();
    std::set<std::uint64_t> allocated{};
    for (std::size_t i=0; i<count; ++i) {
        for (auto id : ids("P" + std::to_string(i))) EXPECT_TRUE(allocated.insert(id).second);
    }
    EXPECT_EQ(count * 2, allocated.size());
}

TEST_F(definition_id_test, legacy_missing_ids_are_assigned_once_and_persisted) {
    if (kvs::implementation_id() == "memory") GTEST_SKIP();
    execute_statement("CREATE TABLE T(K BIGINT PRIMARY KEY)");
    auto key = global::storage_manager()->get_storage_key("T");
    ASSERT_TRUE(key);
    {
        auto storage = global::db()->get_storage(*key);
        ASSERT_TRUE(storage);
        sharksfin::StorageOptions options{};
        ASSERT_EQ(status::ok, storage->get_options(options));
        proto::metadata::storage::Storage metadata{};
        ASSERT_TRUE(metadata.ParseFromString(std::string{options.payload()}));
        metadata.mutable_index()->clear_definition_id_optional();
        metadata.mutable_index()->mutable_table_definition()->clear_definition_id_optional();
        std::string payload{};
        ASSERT_TRUE(metadata.SerializeToString(&payload));
        options.payload(std::move(payload));
        ASSERT_EQ(status::ok, storage->set_options(options));
    }
    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());
    auto migrated = ids("T");
    EXPECT_NE(migrated[0], migrated[1]);
    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());
    EXPECT_EQ(migrated, ids("T"));
}

TEST_F(definition_id_test, overflow_does_not_consume_or_wrap_ids) {
    std::uint64_t first{}, next{};
    auto* allocator = db_impl()->definition_ids();
    ASSERT_EQ(status::ok, allocator->reserve(1, 1, first));
    EXPECT_EQ(status::err_resource_limit_reached, allocator->reserve(std::numeric_limits<std::uint64_t>::max(), 1, next));
    ASSERT_EQ(status::ok, allocator->reserve(1, 1, next));
    EXPECT_EQ(first + 1, next);
}

TEST_F(definition_id_test, missing_counter_is_an_error_not_a_reset_or_partial_create) {
    auto counter = global::db()->get_storage(storage::definition_id_allocator::storage_name);
    ASSERT_TRUE(counter);
    auto tx = global::db()->create_transaction();
    ASSERT_TRUE(tx);
    ASSERT_EQ(status::ok, counter->content_delete(*tx, "next"));
    ASSERT_EQ(status::ok, tx->commit());
    execute_statement("CREATE TABLE T(K BIGINT PRIMARY KEY)", status::err_invalid_state);
    EXPECT_FALSE(db_impl()->tables()->find_table("T"));
    EXPECT_FALSE(global::storage_manager()->find_by_name("T"));
}

TEST_F(definition_id_test, durability_progress_does_not_require_another_sql_worker) {
    ASSERT_EQ(status::ok, db_->stop());
    db_impl()->config()->thread_pool_size(1);
    db_impl()->config()->omit_task_when_idle(false);
    ASSERT_EQ(status::ok, db_->start());
    execute_statement("CREATE TABLE T(K BIGINT PRIMARY KEY)");
    EXPECT_LT(ids("T")[0], ids("T")[1]);
}

TEST_F(definition_id_test, maximum_existing_id_survives_recovery_and_truncate) {
    if (kvs::implementation_id() == "memory") GTEST_SKIP();
    execute_statement("CREATE TABLE T(K BIGINT PRIMARY KEY)");
    auto key = global::storage_manager()->get_storage_key("T");
    ASSERT_TRUE(key);
    {
        auto target = global::db()->get_storage(*key);
        ASSERT_TRUE(target);
        sharksfin::StorageOptions options{};
        ASSERT_EQ(status::ok, target->get_options(options));
        proto::metadata::storage::Storage metadata{};
        ASSERT_TRUE(metadata.ParseFromString(std::string{options.payload()}));
        metadata.mutable_index()->set_definition_id(std::numeric_limits<std::uint64_t>::max());
        metadata.mutable_index()->mutable_table_definition()->set_definition_id(0);
        std::string payload{};
        ASSERT_TRUE(metadata.SerializeToString(&payload));
        options.payload(std::move(payload));
        ASSERT_EQ(status::ok, target->set_options(options));
    }
    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());
    EXPECT_EQ((std::array<std::uint64_t, 2>{0, std::numeric_limits<std::uint64_t>::max()}), ids("T"));
    execute_statement("INSERT INTO T VALUES(1)");
    std::vector<mock::basic_record> records{};
    execute_query("SELECT K FROM T", records);
    ASSERT_EQ(1, records.size());
    execute_statement("TRUNCATE TABLE T");
    EXPECT_EQ((std::array<std::uint64_t, 2>{0, std::numeric_limits<std::uint64_t>::max()}), ids("T"));
    execute_statement("CREATE TABLE OTHER(K BIGINT PRIMARY KEY)", status::err_resource_limit_reached);
    EXPECT_FALSE(db_impl()->tables()->find_table("OTHER"));
    EXPECT_FALSE(global::storage_manager()->find_by_name("OTHER"));
}

TEST_F(definition_id_test, empty_counter_storage_recovers_unfinished_initialization) {
    if (kvs::implementation_id() == "memory") GTEST_SKIP();
    // Simulate a crash after counter storage creation but before its first commit.
    auto counter = global::db()->get_storage(storage::definition_id_allocator::storage_name);
    ASSERT_TRUE(counter);
    auto tx = global::db()->create_transaction();
    ASSERT_TRUE(tx);
    ASSERT_EQ(status::ok, counter->content_delete(*tx, "next"));
    ASSERT_EQ(status::ok, counter->content_delete(*tx, "initialized"));
    ASSERT_EQ(status::ok, tx->commit());
    tx.reset();
    counter.reset();
    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());
    execute_statement("CREATE TABLE T(K BIGINT PRIMARY KEY)");
    auto first = ids("T");
    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());
    EXPECT_EQ(first, ids("T"));
}

TEST_F(definition_id_test, durability_timeout_does_not_publish_or_reuse_a_reserved_id) {
    if (kvs::implementation_id() == "memory") GTEST_SKIP(); // Memory commit marker is always zero.
    std::uint64_t before{}, unpublished{}, after{};
    ASSERT_EQ(status::ok, db_impl()->definition_ids()->reserve(1, 1, before));
    // The separate allocator intentionally has no durability callback registered.
    storage::definition_id_allocator stalled{*global::db(), std::chrono::milliseconds(10)};
    ASSERT_EQ(status::ok, stalled.initialize());
    EXPECT_EQ(status::err_time_out, stalled.reserve(1, 1, unpublished));
    EXPECT_EQ(0, unpublished);
    ASSERT_EQ(status::ok, db_impl()->definition_ids()->reserve(1, 1, after));
    EXPECT_EQ(before + 2, after);
}

}
