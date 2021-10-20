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
#include <future>
#include <gtest/gtest.h>

#include <takatori/util/downcast.h>

#include <jogasaki/executor/common/graph.h>
#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/executor/process/impl/expression/any.h>

#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/utils/mock/storage_data.h>
#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/transaction.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/impl/record.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/executor/tables.h>
#include "api_test_base.h"
#include "../test_utils/temporary_folder.h"
#include <jogasaki/mock/test_channel.h>
#include <jogasaki/utils/mock/msgbuf_utils.h>

#include "request.pb.h"
#include "response.pb.h"
#include "common.pb.h"
#include "schema.pb.h"
#include "status.pb.h"

namespace jogasaki::api {

using namespace std::chrono_literals;
using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;

using takatori::util::unsafe_downcast;

inline jogasaki::meta::record_meta create_record_meta(::schema::RecordMeta const& proto) {
    std::vector<meta::field_type> fields{};
    boost::dynamic_bitset<std::uint64_t> nullities;
    for(std::size_t i=0, n=proto.columns_size(); i<n; ++i) {
        auto& c = proto.columns(i);
        bool nullable = c.nullable();
        meta::field_type field{};
        nullities.push_back(nullable);
        switch(c.type()) {
            using kind = meta::field_type_kind;
            case ::common::DataType::INT4: fields.emplace_back(meta::field_enum_tag<kind::int4>); break;
            case ::common::DataType::INT8: fields.emplace_back(meta::field_enum_tag<kind::int8>); break;
            case ::common::DataType::FLOAT4: fields.emplace_back(meta::field_enum_tag<kind::float4>); break;
            case ::common::DataType::FLOAT8: fields.emplace_back(meta::field_enum_tag<kind::float8>); break;
            case ::common::DataType::CHARACTER: fields.emplace_back(meta::field_enum_tag<kind::character>); break;
        }
    }
    jogasaki::meta::record_meta meta{std::move(fields), std::move(nullities)};
    return meta;
}

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

        auto* impl = db_impl();
        add_benchmark_tables(*impl->tables());
        register_kvs_storage(*impl->kvs_db(), *impl->tables());
    }

    void TearDown() override {
        db_teardown();
    }

};

using namespace std::string_view_literals;

TEST_F(async_api_test, async_insert) {
    std::unique_ptr<api::executable_statement> stmt{};
    ASSERT_EQ(status::ok, db_->create_executable("INSERT INTO T0 (C0, C1) VALUES (1, 20.0)", stmt));
    auto tx = db_->create_transaction();
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
}

TEST_F(async_api_test, async_update) {
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    std::unique_ptr<api::executable_statement> stmt{};
    ASSERT_EQ(status::ok, db_->create_executable("UPDATE T0 SET C1=20.0 WHERE C0=1", stmt));
    auto tx = db_->create_transaction();
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
}

TEST_F(async_api_test, async_query) {
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (2, 20.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (3, 30.0)");
    std::unique_ptr<api::executable_statement> stmt{};
    ASSERT_EQ(status::ok, db_->create_executable("SELECT * FROM T0 ORDER BY C0", stmt));
    auto tx = db_->create_transaction();
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
}

TEST_F(async_api_test, async_query_heavy_write) {
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (2, 20.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (3, 30.0)");
    std::unique_ptr<api::executable_statement> stmt{};
    ASSERT_EQ(status::ok, db_->create_executable("SELECT * FROM T0 ORDER BY C0", stmt));
    auto tx = db_->create_transaction();
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
}

TEST_F(async_api_test, async_query_multi_thread) {
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
    std::vector<std::unique_ptr<transaction>> transactions{};
    transactions.resize(num_thread);
    for(std::size_t i=0; i < num_thread; ++i) {
        vec.emplace_back(
            std::async(std::launch::async, [&, i]() {
                std::unique_ptr<api::executable_statement> stmt{};
                if(auto rc = db_->create_executable("SELECT * FROM T0", stmt); rc != status::ok) {
                    std::abort();
                }
                transactions[i] = db_->create_transaction();
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

}
