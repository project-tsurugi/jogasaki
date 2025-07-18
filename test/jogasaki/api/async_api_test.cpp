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

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cxxabi.h>
#include <future>
#include <memory>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>
#include <boost/move/utility_core.hpp>
#include <gtest/gtest.h>

#include <takatori/util/downcast.h>
#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/api/executable_statement.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/configuration.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/kvs/id.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/mock/test_channel.h>
#include <jogasaki/model/port.h>
#include <jogasaki/proto/sql/common.pb.h>
#include <jogasaki/request_context.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/status.h>
#include <jogasaki/utils/create_tx.h>
#include <jogasaki/utils/interference_size.h>
#include <jogasaki/utils/msgbuf_utils.h>
#include <jogasaki/utils/tables.h>

#include "api_test_base.h"

namespace jogasaki::api {

using namespace std::chrono_literals;
using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::utils;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;

using takatori::util::unsafe_downcast;
namespace sql = jogasaki::proto::sql;

class async_api_test :
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
        auto& impl = jogasaki::api::impl::get_impl(*db_);
        jogasaki::utils::add_test_tables(*impl.tables());
        jogasaki::executor::register_kvs_storage(*impl.kvs_db(), *impl.tables());
    }

    void TearDown() override {
        db_teardown();
    }

};

using namespace std::string_view_literals;

TEST_F(async_api_test, async_insert) {
    std::unique_ptr<api::executable_statement> stmt{};
    ASSERT_EQ(status::ok, db_->create_executable("INSERT INTO T0 (C0, C1) VALUES (1, 20.0)", stmt));
    auto tx = utils::create_transaction(*db_);
    status s{};
    std::string message{"message"};
    std::atomic_bool run{false};
    tx->execute_async(maybe_shared_ptr{stmt.get()}, [&](status st, std::string_view msg){
        s = st;
        message = msg;
        run.store(true);
    });
    while(! run.load()) {}
    ASSERT_EQ(status::ok, s);
    ASSERT_TRUE(message.empty());
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(async_api_test, async_update) {
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    std::unique_ptr<api::executable_statement> stmt{};
    ASSERT_EQ(status::ok, db_->create_executable("UPDATE T0 SET C1=20.0 WHERE C0=1", stmt));
    auto tx = utils::create_transaction(*db_);
    status s{};
    std::string message{"message"};
    std::atomic_bool run{false};
    tx->execute_async(maybe_shared_ptr{stmt.get()}, [&](status st, std::string_view msg){
        s = st;
        message = msg;
        run.store(true);
    });
    while(! run.load()) {}
    ASSERT_EQ(status::ok, s);
    ASSERT_TRUE(message.empty());
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(async_api_test, async_query) {
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (2, 20.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (3, 30.0)");
    std::unique_ptr<api::executable_statement> stmt{};
    ASSERT_EQ(status::ok, db_->create_executable("SELECT * FROM T0 ORDER BY C0", stmt));
    auto tx = utils::create_transaction(*db_);
    status s{};
    std::string message{"message"};
    std::atomic_bool run{false};
    test_channel ch{};
    ASSERT_TRUE(tx->execute_async(
        maybe_shared_ptr{stmt.get()},
        maybe_shared_ptr{&ch},
        [&](status st, std::string_view msg){
            s = st;
            message = msg;
            run.store(true);
        }
    ));
    while(! run.load()) {}
    ASSERT_EQ(status::ok, s);
    ASSERT_TRUE(message.empty());
    auto& wrt = ch.writers_[0];
    ASSERT_TRUE(stmt->meta());
    auto& m = *unsafe_downcast<api::impl::record_meta>(stmt->meta());
    auto recs = deserialize_msg({wrt->data_.data(), wrt->size_}, *m.meta());
    ASSERT_EQ(3, recs.size());
    auto exp0 = mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(1, 10.0);
    auto exp1 = mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(2, 20.0);
    auto exp2 = mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(3, 30.0);
    EXPECT_EQ(exp0, recs[0]);
    EXPECT_EQ(exp1, recs[1]);
    EXPECT_EQ(exp2, recs[2]);
    EXPECT_TRUE(ch.all_writers_released());
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(async_api_test, async_query_heavy_write) {
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (2, 20.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (3, 30.0)");
    std::unique_ptr<api::executable_statement> stmt{};
    ASSERT_EQ(status::ok, db_->create_executable("SELECT * FROM T0 ORDER BY C0", stmt));
    auto tx = utils::create_transaction(*db_);
    status s{};
    std::string message{"message"};
    std::atomic_bool run{false};
    test_channel ch{10};
    ASSERT_TRUE(tx->execute_async(
        maybe_shared_ptr{stmt.get()},
        maybe_shared_ptr{&ch},
        [&](status st, std::string_view msg){
            s = st;
            message = msg;
            run.store(true);
        }
    ));
    while(! run.load()) {}
    ASSERT_EQ(status::ok, s);
    ASSERT_TRUE(message.empty());
    EXPECT_TRUE(ch.all_writers_released());
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(async_api_test, async_query_multi_thread) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory causes problem accessing from multiple threads";
    }
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (2, 20.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (3, 30.0)");
    static constexpr std::size_t num_thread = 10;
    std::vector<std::unique_ptr<std::atomic_bool>> finished{};
    finished.reserve(num_thread);
    for(std::size_t i=0; i < num_thread; ++i) {
        finished.emplace_back(std::make_unique<std::atomic_bool>());
    }
    std::vector<std::future<void>> vec{};
    std::vector<std::shared_ptr<transaction_handle>> transactions{};
    transactions.resize(num_thread);
    for(std::size_t i=0; i < num_thread; ++i) {
        vec.emplace_back(
            std::async(std::launch::async, [&, i]() {
                std::unique_ptr<api::executable_statement> stmt{};
                if(auto rc = db_->create_executable("SELECT * FROM T0", stmt); rc != status::ok) {
                    std::abort();
                }
                transactions[i] = utils::create_transaction(*db_, false, false); // TODO this tests only stx now
                status s{};
                std::string message{"message"};
                std::shared_ptr<api::executable_statement> shd(std::move(stmt));
                auto ch = std::make_shared<test_channel>();
                transactions[i]->execute_async(
                    std::move(shd),
                    ch,
                    [&, i](status st, std::string_view msg){
                        s = st;
                        message = msg;
                        finished[i]->store(true);
                        (void)stmt;
                    }
                );
                while (! finished[i]->load()) {
                    std::this_thread::sleep_for(1ms);
                }
                ASSERT_EQ(status::ok, s);
                ASSERT_TRUE(message.empty());
                EXPECT_TRUE(ch->all_writers_released());
                ASSERT_EQ(status::ok,transactions[i]->commit());
            })
        );
    }
    for(auto&& x : vec) {
        (void)x.get();
    }
}

TEST_F(async_api_test, null_channel) {
    // you can pass nullptr channel is reading result is not needed
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (2, 20.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (3, 30.0)");
    std::unique_ptr<api::executable_statement> stmt{};
    ASSERT_EQ(status::ok, db_->create_executable("SELECT * FROM T0 ORDER BY C0", stmt));
    auto tx = utils::create_transaction(*db_);
    status s{};
    std::string message{"message"};
    std::atomic_bool run{false};
    ASSERT_TRUE(tx->execute_async(
        maybe_shared_ptr{stmt.get()},
        nullptr,
        [&](status st, std::string_view msg){
            s = st;
            message = msg;
            run.store(true);
        }
    ));
    while(! run.load()) {}
    ASSERT_EQ(status::ok, s);
    ASSERT_TRUE(message.empty());
    ASSERT_TRUE(stmt->meta());
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(async_api_test, execute_statement_as_query) {
    // it's illegal operation to execute statement without result records (e.g. UPDATE) as query
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    std::unique_ptr<api::executable_statement> stmt{};
    ASSERT_EQ(status::ok, db_->create_executable("UPDATE T0 SET C1=100.0 WHERE C0=1", stmt));
    auto tx = utils::create_transaction(*db_);
    status s{};
    std::string message{"message"};
    std::atomic_bool run{false};
    test_channel ch{10};
    ASSERT_FALSE(tx->execute_async(
        maybe_shared_ptr{stmt.get()},
        maybe_shared_ptr{&ch},
        [&](status st, std::string_view msg){
            s = st;
            message = msg;
            run.store(true);
        }
    ));
    while(! run.load()) {}
    ASSERT_EQ(status::err_illegal_operation, s);
    ASSERT_FALSE(message.empty());
    ASSERT_FALSE(stmt->meta());
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(async_api_test, async_commit_multi_thread) {
    // manually check multiple async commit works
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory causes problem accessing from multiple threads";
    }
    static constexpr std::size_t num_thread = 10;
    std::vector<std::shared_ptr<transaction_handle>> transactions{};
    transactions.resize(num_thread);
    for(std::size_t i=0; i < num_thread; ++i) {
        transactions[i] = utils::create_transaction(*db_, false, false); // TODO this tests only stx now
        execute_statement("INSERT INTO T0 (C0, C1) VALUES ("+std::to_string(i)+", 0.0)", *transactions[i]);
    }
    std::vector<std::unique_ptr<std::atomic_bool>> finished{};
    finished.reserve(num_thread);
    for(std::size_t i=0; i < num_thread; ++i) {
        finished.emplace_back(std::make_unique<std::atomic_bool>());
    }
    std::vector<bool> success{};
    for(std::size_t i=0; i < num_thread; ++i) {
        transactions[i]->commit_async([&finished, i](status st, std::shared_ptr<api::error_info> info) {
            ASSERT_EQ(status::ok, st);
            finished[i]->store(true);
        });
    }
    for(std::size_t i=0; i < num_thread; ++i) {
        while(finished[i]->load() == false) {}
    }
}

}  // namespace jogasaki::api
