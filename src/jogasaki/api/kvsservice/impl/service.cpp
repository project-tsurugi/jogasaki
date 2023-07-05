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
#include <takatori/util/exception.h>
#include "service.h"
#include "jogasaki/api/kvsservice/convert.h"

using takatori::util::throw_exception;

namespace jogasaki::api::kvsservice::impl {

service::service(std::shared_ptr<tateyama::api::configuration::whole> const&,
                 jogasaki::api::kvsservice::store* store) : store_(store) {
}

bool service::start() {
    return true;
}

bool service::shutdown(bool) {
    return true;
}

/*
 * reply methods
 */
static void reply(tateyama::api::server::response_code code, std::string_view body,
                  std::shared_ptr<tateyama::api::server::response> &res) {
    res->code(code);
    res->body(body);
}

static void reply(google::protobuf::Message &message, tateyama::api::server::response_code code,
                  std::shared_ptr<tateyama::api::server::response> &res) {
    std::string s { };
    if (!message.SerializeToString(&s)) {
        throw_exception(std::logic_error{"SerializeToOstream failed"});
    }
    reply(code, s, res);
}

static void reply(tateyama::proto::kvs::response::Response &proto_res, std::shared_ptr<tateyama::api::server::response> &res) {
    reply(proto_res, tateyama::api::server::response_code::success, res);
}

static void set_error(status status, tateyama::proto::kvs::response::Error &error, std::string *message) {
    // TODO status (from sharksfin) is 64bit, code (from Java impl) is 32bit
    error.set_code(static_cast<google::protobuf::int32>(status));
    if (message != nullptr && !message->empty()) {
        error.set_allocated_detail(message);
    }
}

static void set_error(status status, tateyama::proto::kvs::response::Error &error) {
    set_error(status, error, nullptr);
}

/*
 * begin
 */
static transaction_type convert(tateyama::proto::kvs::transaction::Type const type) {
    switch (type) {
        case tateyama::proto::kvs::transaction::TYPE_UNSPECIFIED:
            return transaction_type::occ;
        case tateyama::proto::kvs::transaction::SHORT:
            return transaction_type::occ;
        case tateyama::proto::kvs::transaction::LONG:
            return transaction_type::ltx;
        case tateyama::proto::kvs::transaction::READ_ONLY:
            return transaction_type::read_only;
        default:
            throw_exception(std::logic_error{"unknown transaction::Type"});
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

static void error_begin(status status, std::shared_ptr<tateyama::api::server::response> &res) {
    tateyama::proto::kvs::response::Error error { };
    tateyama::proto::kvs::response::Begin begin { };
    tateyama::proto::kvs::response::Response proto_res { };
    set_error(status, error);
    begin.set_allocated_error(&error);
    proto_res.set_allocated_begin(&begin);
    reply(proto_res, res);
    begin.release_error();
    proto_res.release_begin();
}

void service::command_begin(tateyama::proto::kvs::request::Request const &proto_req,
                                 std::shared_ptr<tateyama::api::server::response> &res) {
    auto &begin = proto_req.begin();
    auto option = convert(begin.transaction_option());
    std::shared_ptr<transaction> tx{};
    auto status = store_->begin_transaction(option, tx);
    if (status == status::ok) {
        success_begin(tx, res);
    } else {
        error_begin(status, res);
    }
}

/*
 * commit
 */
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

static void error_commit(status status, std::shared_ptr<tateyama::api::server::response> &res) {
    tateyama::proto::kvs::response::Error error { };
    tateyama::proto::kvs::response::Commit commit { };
    tateyama::proto::kvs::response::Response proto_res { };
    set_error(status, error);
    commit.set_allocated_error(&error);
    proto_res.set_allocated_commit(&commit);
    reply(proto_res, res);
    commit.release_error();
    proto_res.release_commit();
}

void service::command_commit(tateyama::proto::kvs::request::Request const&proto_req,
                                  std::shared_ptr<tateyama::api::server::response> &res) {
    auto &commit = proto_req.commit();
    auto &proto_handle = commit.transaction_handle();
    // TODO proto_type = commit.type();
    auto tx = store_->find_transaction(proto_handle.system_id());
    if (tx == nullptr) {
        error_commit(status::err_invalid_argument, res);
        return;
    }
    status status_tx;
    {
        std::unique_lock<std::mutex> lock{tx->transaction_mutex()};
        status_tx = tx->commit();
    }
    // TODO check transaction status before dispose
    status status_store = store_->dispose_transaction(tx->system_id());
    status status = convert(status_tx, status_store);
    if (status == status::ok) {
        success_commit(res);
    } else {
        error_commit(status, res);
    }
}

/*
 * rollback
 */
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

static void error_rollback(status status, std::shared_ptr<tateyama::api::server::response> &res) {
    tateyama::proto::kvs::response::Error error { };
    tateyama::proto::kvs::response::Rollback rollback { };
    tateyama::proto::kvs::response::Response proto_res { };
    set_error(status, error);
    rollback.set_allocated_error(&error);
    proto_res.set_allocated_rollback(&rollback);
    reply(proto_res, res);
    rollback.release_error();
    proto_res.release_rollback();
}

void service::command_rollback(tateyama::proto::kvs::request::Request const &proto_req,
                                    std::shared_ptr<tateyama::api::server::response> &res) {
    auto &rollback = proto_req.rollback();
    auto &proto_handle = rollback.transaction_handle();
    auto tx = store_->find_transaction(proto_handle.system_id());
    if (tx == nullptr) {
        error_rollback(status::err_invalid_argument, res);
        return;
    }
    status status_tx;
    {
        std::unique_lock<std::mutex> lock{tx->transaction_mutex()};
        status_tx = tx->abort();
    }
    // TODO check transaction status before dispose
    status status_store = store_->dispose_transaction(tx->system_id());
    status status = convert(status_tx, status_store);
    if (status == status::ok) {
        success_rollback(res);
    } else {
        error_rollback(status, res);
    }
}

/*
 * close_transaction
 */
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

static void error_close_transaction(status status, std::shared_ptr<tateyama::api::server::response> &res) {
    tateyama::proto::kvs::response::Error error { };
    tateyama::proto::kvs::response::CloseTransaction close { };
    tateyama::proto::kvs::response::Response proto_res { };
    set_error(status, error);
    close.set_allocated_error(&error);
    proto_res.set_allocated_close_transaction(&close);
    reply(proto_res, res);
    close.release_error();
    proto_res.release_close_transaction();
}

void service::command_close_transaction(tateyama::proto::kvs::request::Request const &proto_req,
                               std::shared_ptr<tateyama::api::server::response> &res) {
    auto &close = proto_req.close_transaction();
    auto &proto_handle = close.transaction_handle();
    auto status = store_->dispose_transaction(proto_handle.system_id());
    if (status == status::ok) {
        success_close_transaction(res);
    } else {
        error_close_transaction(status, res);
    }
}

/*
 * put
 */
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
            throw_exception(std::logic_error{"unknown Put_Type"});
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

static void error_put(status status, std::shared_ptr<tateyama::api::server::response> &res) {
    tateyama::proto::kvs::response::Error error { };
    tateyama::proto::kvs::response::Put put { };
    tateyama::proto::kvs::response::Response proto_res { };
    set_error(status, error);
    put.set_allocated_error(&error);
    proto_res.set_allocated_put(&put);
    reply(proto_res, res);
    put.release_error();
    proto_res.release_put();
}

void service::command_put(tateyama::proto::kvs::request::Request const &proto_req,
                               std::shared_ptr<tateyama::api::server::response> &res) {
    auto &put = proto_req.put();
    if (put.records_size() != 1) {
        error_put(status::err_unsupported, res);
        return;
    }
    auto &proto_handle = put.transaction_handle();
    auto tx = store_->find_transaction(proto_handle.system_id());
    if (tx == nullptr) {
        error_put(status::err_invalid_argument, res);
        return;
    }
    auto &table = put.index().table_name();
    put_option opt = convert(put.type());
    auto &record = put.records(0);
    status status;
    {
        std::unique_lock<std::mutex> lock{tx->transaction_mutex()};
        status = tx->put(table, record, opt);
    }
    switch (status) {
        case status::ok:
        case status::not_found: //opt==update && newly put successfully
        case status::already_exists: // opt==create && updated successfully
            success_put(1, res);
            break;
        default:
            error_put(status, res);
            break;
    }
}

/*
 * get
 */
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

static void error_get(status status, std::shared_ptr<tateyama::api::server::response> &res) {
    tateyama::proto::kvs::response::Error error { };
    tateyama::proto::kvs::response::Get get { };
    tateyama::proto::kvs::response::Response proto_res { };
    set_error(status, error);
    get.set_allocated_error(&error);
    proto_res.set_allocated_get(&get);
    reply(proto_res, res);
    get.release_error();
    proto_res.release_get();
}

void service::command_get(tateyama::proto::kvs::request::Request const &proto_req,
                               std::shared_ptr<tateyama::api::server::response> &res) {
    auto &get = proto_req.get();
    if (get.keys_size() != 1) {
        error_get(status::err_unsupported, res);
        return;
    }
    auto &proto_handle = get.transaction_handle();
    auto tx = store_->find_transaction(proto_handle.system_id());
    if (tx == nullptr) {
        error_get(status::err_invalid_argument, res);
        return;
    }
    auto &table = get.index().table_name();
    tateyama::proto::kvs::response::Get_Success success { };
    auto &key = get.keys(0);
    tateyama::proto::kvs::data::Record record;
    status status;
    {
        std::unique_lock<std::mutex> lock{tx->transaction_mutex()};
        status = tx->get(table, key, record);
    }
    if (status != status::ok) {
        error_get(status, res);
        return;
    }
    success.mutable_records()->AddAllocated(&record);
    success_get(success, res);
    while (success.records_size() > 0) {
        success.mutable_records()->ReleaseLast();
    }
}

/*
 * remove
 */
static remove_option convert(tateyama::proto::kvs::request::Remove_Type type) {
    switch (type) {
        case tateyama::proto::kvs::request::Remove_Type::Remove_Type_REMOVE_TYPE_UNSPECIFIED:
            return remove_option::counting;
        case tateyama::proto::kvs::request::Remove_Type::Remove_Type_COUNTING:
            return remove_option::counting;
        case tateyama::proto::kvs::request::Remove_Type::Remove_Type_INSTANT:
            return remove_option::instant;
        default:
            throw_exception(std::logic_error{"unknown Remove_Type"});
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

static void error_remove(status status, std::shared_ptr<tateyama::api::server::response> &res) {
    tateyama::proto::kvs::response::Error error { };
    tateyama::proto::kvs::response::Remove remove{ };
    tateyama::proto::kvs::response::Response proto_res { };
    set_error(status, error);
    remove.set_allocated_error(&error);
    proto_res.set_allocated_remove(&remove);
    reply(proto_res, res);
    remove.release_error();
    proto_res.release_remove();
}

void service::command_remove(tateyama::proto::kvs::request::Request const &proto_req,
                                  std::shared_ptr<tateyama::api::server::response> &res) {
    auto &remove = proto_req.remove();
    if (remove.keys_size() != 1) {
        error_remove(status::err_unsupported, res);
        return;
    }
    auto &proto_handle = remove.transaction_handle();
    auto tx = store_->find_transaction(proto_handle.system_id());
    if (tx == nullptr) {
        error_remove(status::err_invalid_argument, res);
        return;
    }
    auto &table = remove.index().table_name();
    auto opt = convert(remove.type());
    auto &key = remove.keys(0);
    status status;
    {
        std::unique_lock<std::mutex> lock{tx->transaction_mutex()};
        status = tx->remove(table, key, opt);
    }
    switch (status) {
        case status::ok:
            success_remove(1, res);
            break;
        case status::not_found:
            success_remove(0, res);
            break;
        default:
            error_remove(status, res);
            break;
    }
}

/*
 * service protocol handling
 */
bool service::operator()(std::shared_ptr<tateyama::api::server::request const> req, // NOLINT
                              std::shared_ptr<tateyama::api::server::response> res) {
    tateyama::proto::kvs::request::Request proto_req { };
    res->session_id(req->session_id());
    auto s = req->payload();
    if (!proto_req.ParseFromArray(s.data(), s.size())) {
        reply(tateyama::api::server::response_code::io_error,
              "parse error with request body", res);
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
            reply(tateyama::api::server::response_code::application_error,
                  "not supported yet", res);
            break;
        }
        case tateyama::proto::kvs::request::Request::kBatch: {
            reply(tateyama::api::server::response_code::application_error,
                  "not supported yet", res);
            break;
        }
        case tateyama::proto::kvs::request::Request::COMMAND_NOT_SET: {
            // NOTE: for transfer benchmark of empty message
            // see tsubakuro/modules/kvs/src/bench/java/com/tsurugidb/tsubakuro/kvs/bench/EmptyMessageBench.java
            reply(tateyama::api::server::response_code::success, "", res);
            break;
        }
        default:
            reply(tateyama::api::server::response_code::application_error,
                  "invalid request code", res);
            break;
    }

    return true;
}

}