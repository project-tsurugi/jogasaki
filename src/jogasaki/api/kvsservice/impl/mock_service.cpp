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
#include <jogasaki/logging_helper.h>
#include "mock_service.h"
#include <iostream>
#include <stdio.h>

namespace jogasaki::api::kvsservice::impl {

mock_service::mock_service(std::shared_ptr<tateyama::api::configuration::whole>) {
}

void mock_service::command_begin(tateyama::proto::kvs::request::Request const &proto_req,
        std::shared_ptr<tateyama::api::server::response> const &res) {
    auto &option = proto_req.begin().transaction_option();
    auto system_id = 1234 + option.inclusive_read_areas_size(); // FIXME

    tateyama::proto::kvs::transaction::Handle handle { };
    tateyama::proto::kvs::response::Begin_Success success { };
    tateyama::proto::kvs::response::Begin begin { };
    handle.set_system_id(system_id);
    success.set_allocated_transaction_handle(&handle);
    begin.set_allocated_success(&success);
    //
    std::stringstream ss { };
    if (!begin.SerializeToOstream(&ss)) {
        // throw_exception(std::logic_error{"SerializeToOstream failed"});
    }
    res->code(tateyama::api::server::response_code::success);
    res->body(ss.str());
    std::cout << "mock_service::command_begin" << std::endl;
    //
    begin.release_success();
    success.release_transaction_handle();
}

void mock_service::command_commit(tateyama::proto::kvs::request::Request const&,
        std::shared_ptr<tateyama::api::server::response> const &res) {
    tateyama::proto::kvs::response::Commit commit { };
    tateyama::proto::kvs::response::Void v { };
    commit.set_allocated_success(&v);
    //
    std::stringstream ss { };
    if (!commit.SerializeToOstream(&ss)) {
        // throw_exception(std::logic_error{"SerializeToOstream failed"});
    }
    res->code(tateyama::api::server::response_code::success);
    res->body(ss.str());
    std::cout << "mock_service::command_commit" << std::endl;
    //
    commit.release_success();
}

void mock_service::command_rollback(tateyama::proto::kvs::request::Request const&,
        std::shared_ptr<tateyama::api::server::response> const &res) {
    tateyama::proto::kvs::response::Rollback rollback { };
    tateyama::proto::kvs::response::Void v { };
    rollback.set_allocated_success(&v);
    //
    std::stringstream ss { };
    if (!rollback.SerializeToOstream(&ss)) {
        // throw_exception(std::logic_error{"SerializeToOstream failed"});
    }
    res->code(tateyama::api::server::response_code::success);
    res->body(ss.str());
    std::cout << "mock_service::command_rollback" << std::endl;
    //
    rollback.release_success();
}

static void dump_record(const ::google::protobuf::RepeatedPtrField<::tateyama::proto::kvs::data::Record> &records) {
    std::cout << records.size() << std::endl;
    for (const auto &r : records) {
        auto n = r.names_size();
        for (auto i = 0; i < n; i++) {
            std::cout << r.names(i);
            const auto &v = r.values(i);
            std::cout << "\t" << v.value_case() << ": ";
            std::cout << std::to_string(v.int8_value());
            std::cout << std::endl;
        }
    }
}

void mock_service::command_put(tateyama::proto::kvs::request::Request const &proto_req,
        std::shared_ptr<tateyama::api::server::response> const &res) {
    dump_record(proto_req.put().records());
    //
    tateyama::proto::kvs::response::Put put { };
    tateyama::proto::kvs::response::Put_Success success { };
    success.set_written(proto_req.put().records_size());
    put.set_allocated_success(&success);
    //
    std::stringstream ss { };
    if (!put.SerializeToOstream(&ss)) {
        // throw_exception(std::logic_error{"SerializeToOstream failed"});
    }
    res->code(tateyama::api::server::response_code::success);
    res->body(ss.str());
    std::cout << "mock_service::command_put: " << std::to_string(proto_req.put().records_size()) << std::endl;
    //
    put.release_success();
}

void mock_service::command_get(tateyama::proto::kvs::request::Request const &proto_req,
        std::shared_ptr<tateyama::api::server::response> const &res) {
    dump_record(proto_req.get().keys());
    //
    tateyama::proto::kvs::response::Get get { };
    tateyama::proto::kvs::response::Get_Success success { };
    success.mutable_records()->CopyFrom(proto_req.get().keys());
    get.set_allocated_success(&success);
    //
    std::stringstream ss { };
    if (!get.SerializeToOstream(&ss)) {
        // throw_exception(std::logic_error{"SerializeToOstream failed"});
    }
    res->code(tateyama::api::server::response_code::success);
    res->body(ss.str());
    std::cout << "mock_service::command_get: " << std::to_string(proto_req.get().keys_size()) << std::endl;
    //
    success.mutable_records()->ExtractSubrange(0, success.mutable_records()->size(), nullptr);
    get.release_success();
}

void mock_service::command_remove(tateyama::proto::kvs::request::Request const &proto_req,
        std::shared_ptr<tateyama::api::server::response> const &res) {
    dump_record(proto_req.remove().keys());
    //
    tateyama::proto::kvs::response::Remove remove { };
    tateyama::proto::kvs::response::Remove_Success success { };
    success.set_removed(proto_req.remove().keys_size());
    remove.set_allocated_success(&success);
    //
    std::stringstream ss { };
    if (!remove.SerializeToOstream(&ss)) {
        // throw_exception(std::logic_error{"SerializeToOstream failed"});
    }
    res->code(tateyama::api::server::response_code::success);
    res->body(ss.str());
    std::cout << "mock_service::command_remove: " << std::to_string(proto_req.remove().keys_size()) << std::endl;
    //
    remove.release_success();
}

bool mock_service::operator()(std::shared_ptr<tateyama::api::server::request const> req,
        std::shared_ptr<tateyama::api::server::response> res) {
    tateyama::proto::kvs::request::Request proto_req { };
    res->session_id(req->session_id());
    auto s = req->payload();
    if (!proto_req.ParseFromArray(s.data(), s.size())) {
        res->code(tateyama::api::server::response_code::io_error);
        std::string msg { "parse error with request body" };
        res->body(msg);
        return true;
    }
    switch (proto_req.command_case()) {
    case tateyama::proto::kvs::request::Request::kBegin: {
        command_begin(proto_req, res);
        break;
    }
    case tateyama::proto::kvs::request::Request::kCommit: {
        command_commit(proto_req, res);
        break;
    }
    case tateyama::proto::kvs::request::Request::kRollback: {
        command_rollback(proto_req, res);
        break;
    }
    case tateyama::proto::kvs::request::Request::kCloseTransaction: {
        break;
    }
    case tateyama::proto::kvs::request::Request::kGet: {
        command_get(proto_req, res);
        break;
    }
    case tateyama::proto::kvs::request::Request::kPut: {
        command_put(proto_req, res);
        break;
    }
    case tateyama::proto::kvs::request::Request::kRemove: {
        command_remove(proto_req, res);
        break;
    }
    case tateyama::proto::kvs::request::Request::kScan: {
        break;
    }
    case tateyama::proto::kvs::request::Request::kBatch: {
        break;
    }
    default:
        std::string msg { "invalid request code: " };
        res->code(tateyama::api::server::response_code::io_error);
        res->body(msg);
        break;
    }

    return true;
}

bool mock_service::start() {
    return true;
}

bool mock_service::shutdown(bool) {
    return true;
}

}
