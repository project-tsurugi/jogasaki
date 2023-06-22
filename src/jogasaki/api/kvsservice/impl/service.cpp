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

static void reply(tateyama::proto::kvs::response::UnknownError &proto_err, std::shared_ptr<tateyama::api::server::response> &res) {
    reply(proto_err, tateyama::api::server::response_code::application_error, res);
}

static std::string_view to_string_view(status value) noexcept {
    switch (value) {
        case status::ok: return "ok";
        case status::not_found: return "not_found";
        case status::already_exists: return "already_exists";
        case status::user_rollback: return "user_rollback";
        case status::waiting_for_other_transaction: return "waiting_for_other_transaction";
        case status::err_unknown: return "err_unknown";
        case status::err_io_error: return "err_io_error";
        case status::err_invalid_argument: return "err_invalid_argument";
        case status::err_invalid_state: return "err_invalid_state";
        case status::err_unsupported: return "err_unsupported";
        case status::err_user_error: return "err_user_error";
        case status::err_aborted: return "err_aborted";
        case status::err_aborted_retryable: return "err_aborted_retryable";
        case status::err_time_out: return "err_time_out";
        case status::err_not_implemented: return "err_not_implemented";
        case status::err_illegal_operation: return "err_illegal_operation";
        case status::err_conflict_on_write_preserve: return "err_conflict_on_write_preserve";
        case status::err_write_without_write_preserve: return "err_write_without_write_preserve";
        case status::err_inactive_transaction: return "err_inactive_transaction";
        case status::err_blocked_by_concurrent_operation: return "err_blocked_by_concurrent_operation";
        case status::err_resource_limit_reached: return "err_resource_limit_reached";
        case status::err_invalid_key_length: return "err_invalid_key_length";
        default: return "FIXME PROGRAM_ERROR"; // FIXME
    }
}

static void reply(status value, std::string_view msg, std::shared_ptr<tateyama::api::server::response> &res) {
    // FIXME
    std::string s {to_string_view(value) };
    if (msg.size() > 0) {
        s += ": ";
        s += msg;
    }
    tateyama::proto::kvs::response::UnknownError proto_err{};
    proto_err.set_message(s);
    reply(proto_err, res);
    proto_err.release_message();
}

static void reply(status value, std::shared_ptr<tateyama::api::server::response> &res) {
    reply(value, {}, res);
}

static void command_not_supported(tateyama::proto::kvs::request::Request const &proto_req,
                                  std::shared_ptr<tateyama::api::server::response> &res) {
    // FIXME
    reply(status::err_unsupported, std::to_string(proto_req.command_case()), res);
}

/*
 * begin
 */
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

/*
 * commit
 */
void service::command_begin(tateyama::proto::kvs::request::Request const &proto_req,
                                 std::shared_ptr<tateyama::api::server::response> &res) {
    auto &begin = proto_req.begin();
    auto option = convert(begin.transaction_option());
    std::shared_ptr<transaction> tx{};
    auto status = store_->begin_transaction(option, tx);
    if (status == status::ok) {
        success_begin(tx, res);
    } else {
        // FIXME
        reply(status, res);
    }
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
    auto &commit = proto_req.commit();
    auto &proto_handle = commit.transaction_handle();
    // FIXME
    // auto proto_type = commit.type();
    auto tx = store_->find_transaction(proto_handle.system_id());
    if (tx == nullptr) {
        // FIXME
        reply(status::err_invalid_argument, res);
        return;
    }
    status status_tx;
    {
        std::unique_lock<std::mutex> lock{tx->transaction_mutex()};
        status_tx = tx->commit();
    }
    // FIXME
    status status_store = store_->dispose_transaction(tx->system_id());
    status status = (status_tx != status::ok ? status_tx : status_store);
    if (status == status::ok) {
        success_commit(res);
    } else {
        // FIXME
        reply(status, res);
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

void service::command_rollback(tateyama::proto::kvs::request::Request const &proto_req,
                                    std::shared_ptr<tateyama::api::server::response> &res) {
    auto &rollback = proto_req.rollback();
    auto &proto_handle = rollback.transaction_handle();
    auto tx = store_->find_transaction(proto_handle.system_id());
    if (tx == nullptr) {
        // FIXME
        reply(status::err_invalid_argument, res);
        return;
    }
    status status_tx;
    {
        std::unique_lock<std::mutex> lock{tx->transaction_mutex()};
        status_tx = tx->abort();
    }
    // FIXME
    status status_store = store_->dispose_transaction(tx->system_id());
    status status = (status_tx != status::ok ? status_tx : status_store);
    if (status == status::ok) {
        success_rollback(res);
    } else {
        // FIXME
        reply(status, res);
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

void service::command_close_transaction(tateyama::proto::kvs::request::Request const &proto_req,
                               std::shared_ptr<tateyama::api::server::response> &res) {
    auto &close = proto_req.close_transaction();
    auto &proto_handle = close.transaction_handle();
    auto status = store_->dispose_transaction(proto_handle.system_id());
    if (status == status::ok) {
        success_close_transaction(res);
    } else {
        // FIXME
        reply(status, res);
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
    auto &put = proto_req.put();
    if (put.records_size() != 1) {
        // FIXME
        command_not_supported(proto_req, res);
        return;
    }
    auto &proto_handle = put.transaction_handle();
    auto tx = store_->find_transaction(proto_handle.system_id());
    if (tx == nullptr) {
        // FIXME
        reply(status::err_invalid_argument, res);
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
    if (status != status::ok) {
        reply(status, res);
        return;
    }
    success_put(1, res);
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

void service::command_get(tateyama::proto::kvs::request::Request const &proto_req,
                               std::shared_ptr<tateyama::api::server::response> &res) {
    auto &get = proto_req.get();
    if (get.keys_size() != 1) {
        // FIXME
        command_not_supported(proto_req, res);
        return;
    }
    auto &proto_handle = get.transaction_handle();
    auto tx = store_->find_transaction(proto_handle.system_id());
    if (tx == nullptr) {
        // FIXME
        reply(status::err_invalid_argument, res);
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
        // FIXME
        reply(status, res);
        return;
    }
    success.mutable_records()->AddAllocated(&record);
    success_get(success, res);
    //
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
    auto &remove = proto_req.remove();
    if (remove.keys_size() != 1) {
        // FIXME
        command_not_supported(proto_req, res);
        return;
    }
    auto &proto_handle = remove.transaction_handle();
    auto tx = store_->find_transaction(proto_handle.system_id());
    if (tx == nullptr) {
        // FIXME
        reply(status::err_invalid_argument, res);
        return;
    }
    auto &table = remove.index().table_name();
    auto opt = convert(remove.type());
    auto removed = 0;
    for (auto &key : remove.keys()) {
        status status;
        {
            // FIXME
            std::unique_lock<std::mutex> lock{tx->transaction_mutex()};
            status = tx->remove(table, key, opt);
        }
        if (status == status::ok) {
            removed++;
        } else {
            // FIXME
        }
    }
    success_remove(removed, res);
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
            command_not_supported(proto_req, res);
            break;
        }
        case tateyama::proto::kvs::request::Request::kBatch: {
            command_not_supported(proto_req, res);
            break;
        }
        default:
            reply(tateyama::api::server::response_code::io_error,
                  "invalid request code", res);
            break;
    }

    return true;
}

}
