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
#include "service.h"

namespace jogasaki::api::kvsservice::impl {

service::service(std::shared_ptr<tateyama::api::configuration::whole> const&,
                 jogasaki::api::kvsservice::store* store) : store_(store) {
}

static transaction_type convert(tateyama::proto::kvs::transaction::Type const type) {
    switch (type) {
        case tateyama::proto::kvs::transaction::TYPE_UNSPECIFIED:
            return transaction_type::unspecified;
        case tateyama::proto::kvs::transaction::SHORT:
            return transaction_type::occ;
        case tateyama::proto::kvs::transaction::LONG:
            return transaction_type::ltx;
        case tateyama::proto::kvs::transaction::READ_ONLY:
            return transaction_type::read_only;
        default:
            // FIXME
            return transaction_type::unspecified;
    }
}

static table_areas convert(google::protobuf::RepeatedPtrField<tateyama::proto::kvs::transaction::TableArea> const &proto_areas) {
    table_areas areas {};
    for (auto &area : proto_areas) {
        areas.emplace_back(area.table_name());
    }
    return areas;
}

static transaction_option convert(tateyama::proto::kvs::transaction::Option const &proto_opt) {
    auto type = convert(proto_opt.type());
    auto write_preserves = convert(proto_opt.write_preserves());
    transaction_option opt (type, std::move(write_preserves));
    return opt;
}

static void reply(tateyama::proto::kvs::response::Response &proto_res, std::shared_ptr<tateyama::api::server::response> &res) {
    std::string s { };
    if (!proto_res.SerializeToString(&s)) {
        // FIXME throw_exception(std::logic_error{"SerializeToOstream failed"});
    }
    res->code(tateyama::api::server::response_code::success);
    res->body(s);
}

static void success_begin(std::shared_ptr<transaction> const &tx, std::shared_ptr<tateyama::api::server::response> &res) {
    tateyama::proto::kvs::transaction::Handle handle { };
    tateyama::proto::kvs::response::Begin_Success success { };
    tateyama::proto::kvs::response::Begin begin { };
    tateyama::proto::kvs::response::Response proto_res { };
    handle.set_system_id(tx->system_id());
    success.set_allocated_transaction_handle(&handle);
    begin.set_allocated_success(&success);
    proto_res.set_allocated_begin(&begin);
    reply(proto_res, res);
    begin.release_success();
    success.release_transaction_handle();
    proto_res.release_begin();
}

void service::command_begin(tateyama::proto::kvs::request::Request const &proto_req,
                                 std::shared_ptr<tateyama::api::server::response> &res) {
    auto &proto_opt = proto_req.begin().transaction_option();
    auto option = convert(proto_opt);
    std::shared_ptr<transaction> tx{};
    auto status = store_->begin_transaction(option, tx);
    if (status != status::ok) {
        // FIXME
        // tateyama::proto::kvs::response::UnknownError unknown {};
        // unknown.set_message();
    }
    success_begin(tx, res);
}

static void success_commit(std::shared_ptr<tateyama::api::server::response> &res) {
    tateyama::proto::kvs::response::Commit commit { };
    tateyama::proto::kvs::response::Void v { };
    tateyama::proto::kvs::response::Response proto_res { };
    commit.set_allocated_success(&v);
    proto_res.set_allocated_commit(&commit);
    reply(proto_res, res);
    commit.release_success();
    proto_res.release_commit();
}

void service::command_commit(tateyama::proto::kvs::request::Request const&proto_req,
                                  std::shared_ptr<tateyama::api::server::response> &res) {
    auto &proto_handle = proto_req.commit().transaction_handle();
    // FIXME
    // auto proto_type = proto_req.commit().type();
    auto tx = store_->find_transaction(proto_handle.system_id());
    if (tx == nullptr) {
        // FIXME
    }
    auto status = tx->commit();
    if (status != status::ok) {
        // FIXME
    }
    success_commit(res);
}

static void success_rollback(std::shared_ptr<tateyama::api::server::response> &res) {
    tateyama::proto::kvs::response::Rollback rollback { };
    tateyama::proto::kvs::response::Void v { };
    tateyama::proto::kvs::response::Response proto_res { };
    rollback.set_allocated_success(&v);
    proto_res.set_allocated_rollback(&rollback);
    reply(proto_res, res);
    rollback.release_success();
    proto_res.release_rollback();
}

void service::command_rollback(tateyama::proto::kvs::request::Request const &proto_req,
                                    std::shared_ptr<tateyama::api::server::response> &res) {
    auto &proto_handle = proto_req.rollback().transaction_handle();
    // FIXME
    auto tx = store_->find_transaction(proto_handle.system_id());
    if (tx == nullptr) {
        // FIXME
    }
    auto status = tx->abort();
    if (status != status::ok) {
        // FIXME
    }
    success_rollback(res);
}

static void success_close_transaction(std::shared_ptr<tateyama::api::server::response> &res) {
    tateyama::proto::kvs::response::CloseTransaction close { };
    tateyama::proto::kvs::response::Void v { };
    tateyama::proto::kvs::response::Response proto_res { };
    close.set_allocated_success(&v);
    proto_res.set_allocated_close_transaction(&close);
    reply(proto_res, res);
    close.release_success();
    proto_res.release_close_transaction();
}

void service::command_close_transaction(tateyama::proto::kvs::request::Request const &proto_req,
                               std::shared_ptr<tateyama::api::server::response> &res) {
    auto &proto_handle = proto_req.close_transaction().transaction_handle();
    auto status = store_->dispose_transaction(proto_handle.system_id());
    if (status != status::ok) {
        // FIXME
    }
    success_close_transaction(res);
}

static put_option convert(tateyama::proto::kvs::request::Put_Type type) {
    switch (type) {
        case tateyama::proto::kvs::request::Put_Type::Put_Type_PUT_TYPE_UNSPECIFIED:
            return put_option::create_or_update;
        case tateyama::proto::kvs::request::Put_Type::Put_Type_OVERWRITE:
            return put_option::create_or_update;
        case tateyama::proto::kvs::request::Put_Type::Put_Type_IF_ABSENT:
            return put_option::create;
        case tateyama::proto::kvs::request::Put_Type:: Put_Type_IF_PRESENT:
            return put_option::update;
        default:
            // FIXME
            return put_option::create_or_update;
    }
}

static void success_put(int written, std::shared_ptr<tateyama::api::server::response> &res) {
    tateyama::proto::kvs::response::Put put { };
    tateyama::proto::kvs::response::Put_Success success { };
    tateyama::proto::kvs::response::Response proto_res { };
    success.set_written(written);
    put.set_allocated_success(&success);
    proto_res.set_allocated_put(&put);
    reply(proto_res, res);
    put.release_success();
    proto_res.release_put();
}

void service::command_put(tateyama::proto::kvs::request::Request const &proto_req,
                               std::shared_ptr<tateyama::api::server::response> &res) {
    auto &proto_handle = proto_req.put().transaction_handle();
    auto tx = store_->find_transaction(proto_handle.system_id());
    if (tx == nullptr) {
        // FIXME
    }
    auto &table = proto_req.put().index().table_name();
    put_option opt = convert(proto_req.put().type());
    auto written = 0;
    for (auto &record : proto_req.put().records()) {
        auto status = tx->put(table, record, opt);
        if (status == status::ok) {
            written++;
        } else {
            // FIXME
        }
    }
    success_put(written, res);
}

static void success_get(tateyama::proto::kvs::response::Get_Success &success, std::shared_ptr<tateyama::api::server::response> &res) {
    tateyama::proto::kvs::response::Get get { };
    tateyama::proto::kvs::response::Response proto_res { };
    get.set_allocated_success(&success);
    proto_res.set_allocated_get(&get);
    reply(proto_res, res);
    while (success.records_size() > 0) {
        success.mutable_records()->ReleaseLast();
    }
    get.release_success();
    proto_res.release_get();
}

void service::command_get(tateyama::proto::kvs::request::Request const &proto_req,
                               std::shared_ptr<tateyama::api::server::response> &res) {
    auto &proto_handle = proto_req.put().transaction_handle();
    auto tx = store_->find_transaction(proto_handle.system_id());
    if (tx == nullptr) {
        // FIXME
    }
    auto &table = proto_req.get().index().table_name();
    tateyama::proto::kvs::response::Get_Success success { };
    for (auto &key : proto_req.get().keys()) {
        tateyama::proto::kvs::data::Record record;
        auto status = tx->get(table, key, record);
        if (status == status::ok) {
            success.mutable_records()->AddAllocated(&record);
        } else {
            // FIXME
        }
    }
    success_get(success, res);
}

static remove_option convert(tateyama::proto::kvs::request::Remove_Type type) {
    switch (type) {
        case tateyama::proto::kvs::request::Remove_Type::Remove_Type_REMOVE_TYPE_UNSPECIFIED:
            return remove_option::counting;
        case tateyama::proto::kvs::request::Remove_Type::Remove_Type_COUNTING:
            return remove_option::counting;
        case tateyama::proto::kvs::request::Remove_Type::Remove_Type_INSTANT:
            return remove_option::instant;
        default:
            // FIXME
            return remove_option::counting;
    }
}

static void success_remove(int removed, std::shared_ptr<tateyama::api::server::response> &res) {
    tateyama::proto::kvs::response::Remove remove { };
    tateyama::proto::kvs::response::Remove_Success success { };
    tateyama::proto::kvs::response::Response proto_res { };
    success.set_removed(removed);
    remove.set_allocated_success(&success);
    proto_res.set_allocated_remove(&remove);
    reply(proto_res, res);
    remove.release_success();
    proto_res.release_remove();
}

void service::command_remove(tateyama::proto::kvs::request::Request const &proto_req,
                                  std::shared_ptr<tateyama::api::server::response> &res) {
    auto &proto_handle = proto_req.put().transaction_handle();
    auto tx = store_->find_transaction(proto_handle.system_id());
    if (tx == nullptr) {
        // FIXME
    }
    auto &table = proto_req.remove().index().table_name();
    auto opt = convert(proto_req.remove().type());
    auto removed = 0;
    for (auto &record : proto_req.put().records()) {
        auto status = tx->remove(table, record, opt);
        if (status == status::ok) {
            removed++;
        } else {
            // FIXME
        }
    }
    success_remove(removed, res);
}

bool service::operator()(std::shared_ptr<tateyama::api::server::request const> req, // NOLINT
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
            command_close_transaction(proto_req, res);
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

bool service::start() {
    return true;
}

bool service::shutdown(bool) {
    return true;
}

}
