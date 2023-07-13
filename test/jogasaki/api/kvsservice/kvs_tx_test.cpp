/*
 * Copyright 2018-2023 tsurugi project.
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

#include <tateyama/framework/server.h>
#include <tateyama/framework/component_ids.h>
#include <tateyama/loopback/loopback_client.h>

#include <jogasaki/api/service/bridge.h>
#include <jogasaki/api/resource/bridge.h>
#include <jogasaki/api/kvsservice/service.h>
#include <jogasaki/api/kvsservice/resource.h>

#include <tateyama/proto/kvs/data.pb.h>
#include <tateyama/proto/kvs/transaction.pb.h>
#include <tateyama/proto/kvs/request.pb.h>
#include <tateyama/proto/kvs/response.pb.h>

#include "test_utils.h"

namespace jogasaki::api::kvsservice {

std::int64_t now_nsec() noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
}

class kvx_tx_test : public ::testing::Test {
public:
    void SetUp() override {
    }

    void TearDown() override {
    }

    const std::size_t session_id = 123;
    tateyama::framework::component::id_type service_id = tateyama::framework::service_id_remote_kvs;

    tateyama::proto::kvs::transaction::Handle begin(tateyama::loopback::loopback_client &client) {
        std::string s{};
        {
            tateyama::proto::kvs::transaction::Option option {};
            tateyama::proto::kvs::request::Begin begin { };
            tateyama::proto::kvs::request::Request proto_req { };
            option.set_type(tateyama::proto::kvs::transaction::Type::SHORT);
            begin.set_allocated_transaction_option(&option);
            proto_req.set_allocated_begin(&begin);
            EXPECT_TRUE(proto_req.SerializeToString(&s));
            proto_req.release_begin();
            begin.release_transaction_option();
        }
        auto buf_res = client.request(session_id, service_id, s);
        auto body = buf_res.body();
        EXPECT_TRUE(body.size() > 0);
        {
            tateyama::proto::kvs::response::Response proto_res{};
            EXPECT_TRUE(proto_res.ParseFromArray(body.data(), body.size()));
            EXPECT_TRUE(proto_res.has_begin());
            EXPECT_TRUE(proto_res.begin().has_success());
            EXPECT_TRUE(proto_res.begin().success().has_transaction_handle());
            return proto_res.begin().success().transaction_handle();
        }
    }

    void make_record(tateyama::proto::kvs::data::Record &record) {
        tateyama::proto::kvs::data::Value keyValue{};
        tateyama::proto::kvs::data::Value dataValue{};
        record.add_names("key");
        keyValue.set_int8_value(v++);
        record.mutable_values()->AddAllocated(&keyValue);
        record.add_names("value0");
        dataValue.set_int8_value(v++);
        record.mutable_values()->AddAllocated(&dataValue);
    }

    void release_record(tateyama::proto::kvs::data::Record *record) {
        while (record->names_size() > 0) {
            record->mutable_names()->ReleaseLast();
        }
        while (record->values_size() > 0) {
            record->mutable_values()->ReleaseLast();
        }
    }

    void put(tateyama::loopback::loopback_client &client, tateyama::proto::kvs::transaction::Handle &handle) {
        std::string s{};
        {
            tateyama::proto::kvs::data::Record record{};
            tateyama::proto::kvs::request::Index index{};
            tateyama::proto::kvs::request::Put put{};
            tateyama::proto::kvs::request::Request proto_req{};
            make_record(record);
            index.set_table_name("table1");
            put.set_type(tateyama::proto::kvs::request::Put_Type::Put_Type_OVERWRITE);
            put.set_allocated_transaction_handle(&handle);
            put.set_allocated_index(&index);
            put.mutable_records()->AddAllocated(&record);
            proto_req.set_allocated_put(&put);
            EXPECT_TRUE(proto_req.SerializeToString(&s));
            proto_req.release_put();
            put.release_index();
            put.release_transaction_handle();
            while (put.records_size() > 0) {
                auto r = put.mutable_records(put.records_size()-1);
                release_record(r);
                put.mutable_records()->ReleaseLast();
            }
        }
        auto buf_res = client.request(session_id, service_id, s);
        auto body = buf_res.body();
        EXPECT_TRUE(body.size() > 0);
        {
            tateyama::proto::kvs::response::Response proto_res{};
            EXPECT_TRUE(proto_res.ParseFromArray(body.data(), body.size()));
            EXPECT_TRUE(proto_res.has_put());
            EXPECT_TRUE(proto_res.put().has_success());
            EXPECT_EQ(1, proto_res.put().success().written());
        }
    }

    void get(tateyama::loopback::loopback_client &client, tateyama::proto::kvs::transaction::Handle &handle) {
        std::string s{};
        {
            tateyama::proto::kvs::data::Record record{};
            tateyama::proto::kvs::request::Index index{};
            tateyama::proto::kvs::request::Get get{};
            tateyama::proto::kvs::request::Request proto_req{};
            make_record(record);
            index.set_table_name("table1");
            get.set_allocated_transaction_handle(&handle);
            get.set_allocated_index(&index);
            get.mutable_keys()->AddAllocated(&record);
            proto_req.set_allocated_get(&get);
            EXPECT_TRUE(proto_req.SerializeToString(&s));
            proto_req.release_get();
            get.release_index();
            get.release_transaction_handle();
            while (get.keys_size() > 0) {
                auto r = get.mutable_keys(get.keys_size()-1);
                release_record(r);
                get.mutable_keys()->ReleaseLast();
            }
        }
        auto buf_res = client.request(session_id, service_id, s);
        auto body = buf_res.body();
        EXPECT_TRUE(body.size() > 0);
        {
            tateyama::proto::kvs::response::Response proto_res{};
            EXPECT_TRUE(proto_res.ParseFromArray(body.data(), body.size()));
            EXPECT_TRUE(proto_res.has_get());
            EXPECT_TRUE(proto_res.get().has_success());
            EXPECT_EQ(1, proto_res.get().success().records_size());
        }
    }

    void commit(tateyama::loopback::loopback_client &client, tateyama::proto::kvs::transaction::Handle &handle) {
        std::string s{};
        {
            tateyama::proto::kvs::request::Commit commit{};
            tateyama::proto::kvs::request::Request proto_req{};
            commit.set_allocated_transaction_handle(&handle);
            proto_req.set_allocated_commit(&commit);
            EXPECT_TRUE(proto_req.SerializeToString(&s));
            proto_req.release_commit();
            commit.release_transaction_handle();
        }
        auto buf_res = client.request(session_id, service_id, s);
        auto body = buf_res.body();
        EXPECT_TRUE(body.size() > 0);
        {
            tateyama::proto::kvs::response::Response proto_res{};
            EXPECT_TRUE(proto_res.ParseFromArray(body.data(), body.size()));
            EXPECT_TRUE(proto_res.has_commit());
            EXPECT_TRUE(proto_res.commit().has_success());
        }
    }

    std::int64_t tx(tateyama::loopback::loopback_client &loopback) {
        auto handle = begin(loopback);
        get(loopback, handle);
        put(loopback, handle);
        get(loopback, handle);
        commit(loopback, handle);
        return v;
    }
private:
    std::int64_t v = now_nsec();
};

TEST_F(kvx_tx_test, DISABLED_tx_check) {
    tateyama::framework::server sv{tateyama::framework::boot_mode::database_server,
                                   default_configuration()};
    tateyama::framework::add_core_components(sv);
    sv.add_resource(std::make_shared<jogasaki::api::resource::bridge>());
    sv.add_service(std::make_shared<jogasaki::api::service::bridge>());
    sv.add_resource(std::make_shared<jogasaki::api::kvsservice::resource>());
    auto service = std::make_shared<jogasaki::api::kvsservice::service>();
    sv.add_service(service);
    tateyama::loopback::loopback_client loopback;
    sv.add_endpoint(loopback.endpoint());
    EXPECT_TRUE(sv.setup());
    EXPECT_TRUE(sv.start());
    EXPECT_NE(tx(loopback), 0);
    EXPECT_TRUE(sv.shutdown());
}
}
