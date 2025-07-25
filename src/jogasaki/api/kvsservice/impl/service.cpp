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
#include "service.h"

#include <mutex>
#include <ostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <glog/logging.h>
#include <google/protobuf/message.h>
#include <google/protobuf/stubs/port.h>

#include <takatori/util/exception.h>
#include <tateyama/proto/kvs/data.pb.h>
#include <tateyama/proto/kvs/response.pb.h>
#include <tateyama/proto/kvs/transaction.pb.h>

#include <jogasaki/api/kvsservice/put_option.h>
#include <jogasaki/api/kvsservice/remove_option.h>
#include <jogasaki/api/kvsservice/status.h>
#include <jogasaki/api/kvsservice/status_message.h>
#include <jogasaki/api/kvsservice/store.h>
#include <jogasaki/api/kvsservice/transaction.h>
#include <jogasaki/api/kvsservice/transaction_option.h>
#include <jogasaki/api/kvsservice/transaction_priority.h>
#include <jogasaki/api/kvsservice/transaction_type.h>
#include <jogasaki/logging.h>

using takatori::util::throw_exception;

namespace jogasaki::api::kvsservice::impl {

constexpr static std::string_view log_location_prefix = "/:jogasaki:api:kvsservice:impl:service ";

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
static void reply(std::string_view body,
                  std::shared_ptr<tateyama::api::server::response> &res) {
    res->body(body);
}

static void reply(google::protobuf::Message &message,
                  std::shared_ptr<tateyama::api::server::response> &res) {
    std::string s { };
    if (!message.SerializeToString(&s)) {
        throw_exception(std::logic_error{"SerializeToOstream failed"});
    }
    reply(s, res);
}

static void set_error(status status, tateyama::proto::kvs::response::Error &error) {
    error.set_code(static_cast<google::protobuf::int32>(status));
}

static bool set_error(status status, std::string *message, tateyama::proto::kvs::response::Error &error) {
    set_error(status, error);
    if (message != nullptr) {
        error.set_allocated_detail(message);
        return true;
    }
    return false;
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

static transaction_priority convert(tateyama::proto::kvs::transaction::Priority const priority) {
    switch (priority) {
        case tateyama::proto::kvs::transaction::Priority::PRIORITY_UNSPECIFIED:
            return transaction_priority::priority_unspecified;
        case tateyama::proto::kvs::transaction::Priority::INTERRUPT:
            return transaction_priority::interrupt;
        case tateyama::proto::kvs::transaction::Priority::WAIT:
            return transaction_priority::wait;
        case tateyama::proto::kvs::transaction::Priority::INTERRUPT_EXCLUDE:
            return transaction_priority::interrupt_exclude;
        case tateyama::proto::kvs::transaction::Priority::WAIT_EXCLUDE:
            return transaction_priority::wait_exclude;
        default:
            throw_exception(std::logic_error{"unknown transaction::Priority"});
    }

}
static transaction_option convert(tateyama::proto::kvs::transaction::Option const &proto_opt) {
    auto type = convert(proto_opt.type());
    auto write_preserves = convert(proto_opt.write_preserves());
    transaction_option opt (type, std::move(write_preserves));
    opt.label(proto_opt.label());
    opt.priority(convert(proto_opt.priority()));
    opt.modifies_definitions(proto_opt.modifies_definitions());
    opt.inclusive_read_areas(convert(proto_opt.inclusive_read_areas()));
    opt.exclusive_read_areas(convert(proto_opt.exclusive_read_areas()));
    return opt;
}

static void success_begin(std::shared_ptr<transaction> const &tx, std::shared_ptr<tateyama::api::server::response> &res) {
    tateyama::proto::kvs::response::Response proto_res { };
    auto* begin = proto_res.mutable_begin();
    auto* success = begin->mutable_success();
    auto* handle = success->mutable_transaction_handle();
    handle->set_system_id(tx->system_id());
    reply(proto_res, res);
}

static void error_begin(status status, std::shared_ptr<tateyama::api::server::response> &res,
                        std::string *message = nullptr) {
    tateyama::proto::kvs::response::Response proto_res { };
    auto* begin = proto_res.mutable_begin();
    auto* error = begin->mutable_error();
    auto alloc_detail = set_error(status, message, *error);
    reply(proto_res, res);
    if (alloc_detail) {
        (void)error->release_detail();
    }
}

static status_message check_supported(transaction_option &opt) {
    // TODO support various options
    if (opt.type() != transaction_type::occ) {
        // TODO DO NOT USE busy wait for LTX in transaction::commit()
        return {status::err_not_implemented,
                "only supported OCC (short) transaction type, others not implemented yet"};
    }
    if (!opt.write_preserves().empty()) {
        return {status::err_not_implemented,
                "'write_preserve' option not implemented yet"};
    }
    if (opt.priority() != transaction_priority::priority_unspecified) {
        return {status::err_not_implemented,
                "'priority' option not implemented yet"};
    }
    if (!opt.label().empty()) {
        return {status::err_not_implemented,
                "'label' option not implemented yet"};
    }
    if (opt.modifies_definitions()) {
        return {status::err_not_implemented,
                "'modify_definitions' option not implemented yet"};
    }
    if (!opt.inclusive_read_areas().empty()) {
        return {status::err_not_implemented,
                "'inclusive_read_area' option not implemented yet"};
    }
    if (!opt.exclusive_read_areas().empty()) {
        return {status::err_not_implemented,
                "'exclusive_read_area' option not implemented yet"};
    }
    return status_message{status::ok};
}

void service::command_begin(tateyama::proto::kvs::request::Request const &proto_req,
                                 std::shared_ptr<tateyama::api::server::response> &res) {
    auto &begin = proto_req.begin();
    auto option = convert(begin.transaction_option());
    if (auto sm = check_supported(option); sm.status_code() != status::ok) {
        error_begin(sm.status_code(), res, sm.message());
        return;
    }
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
    tateyama::proto::kvs::response::Response proto_res { };
    auto* commit = proto_res.mutable_commit();
    auto* v = commit->mutable_success();
    (void) v;
    reply(proto_res, res);
}

static void error_commit(status status, std::shared_ptr<tateyama::api::server::response> &res,
                         std::string *message = nullptr) {
    tateyama::proto::kvs::response::Response proto_res { };
    auto* commit = proto_res.mutable_commit();
    auto* error = commit->mutable_error();
    auto alloc_detail = set_error(status, message, *error);
    reply(proto_res, res);
    if (alloc_detail) {
        (void)error->release_detail();
    }
}

static status_message check_supported(tateyama::proto::kvs::request::CommitStatus const status) {
    switch (status) {
    case tateyama::proto::kvs::request::CommitStatus::COMMIT_STATUS_UNSPECIFIED:
        return status_message{status::ok};
    case tateyama::proto::kvs::request::CommitStatus::ACCEPTED:
        return {status::err_not_implemented, "'ACCEPTED' option not implemented yet"};
    case tateyama::proto::kvs::request::CommitStatus::AVAILABLE:
        return {status::err_not_implemented, "'AVAILABLE' option not implemented yet"};
    case tateyama::proto::kvs::request::CommitStatus::STORED:
        return {status::err_not_implemented, "'STORED' option not implemented yet"};
    case tateyama::proto::kvs::request::CommitStatus::PROPAGATED:
        return {status::err_not_implemented, "'PROPAGATED' option not implemented yet"};
    default:
        throw_exception(std::logic_error{"unknown CommitStatus"});
    }
}

void service::command_commit(tateyama::proto::kvs::request::Request const&proto_req,
                                  std::shared_ptr<tateyama::api::server::response> &res) {
    auto &commit = proto_req.commit();
    if (auto sm = check_supported(commit.notification_type()); sm.status_code() != status::ok) {
        error_commit(sm.status_code(), res, sm.message());
        return;
    }
    // TODO support commit type
    auto &proto_handle = commit.transaction_handle();
    auto tx = store_->find_transaction(proto_handle.system_id());
    if (tx == nullptr) {
        error_commit(status::err_invalid_argument, res);
        return;
    }
    status status{};
    {
        std::unique_lock<std::mutex> lock{tx->transaction_mutex()};
        status = tx->commit();
    }
    if (status == status::ok && commit.auto_dispose()) {
        auto s = store_->dispose_transaction(proto_handle.system_id());
        if (s != status::ok) {
            VLOG(log_error) << log_location_prefix << "unexpected error destroying transaction: " << s;
        }
    }
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
    tateyama::proto::kvs::response::Response proto_res { };
    auto* rollback = proto_res.mutable_rollback();
    auto* v = rollback->mutable_success();
    (void) v;
    reply(proto_res, res);
}

static void error_rollback(status status, std::shared_ptr<tateyama::api::server::response> &res) {
    tateyama::proto::kvs::response::Response proto_res { };
    auto* rollback = proto_res.mutable_rollback();
    auto* error = rollback->mutable_error();
    set_error(status, *error);
    reply(proto_res, res);
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
    status status{};
    {
        std::unique_lock<std::mutex> lock{tx->transaction_mutex()};
        status = tx->abort();
    }
    // TODO check transaction status before dispose
    if (status == status::ok) {
        success_rollback(res);
    } else {
        error_rollback(status, res);
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
    tateyama::proto::kvs::response::Response proto_res { };
    auto* put = proto_res.mutable_put();
    auto* success = put->mutable_success();
    success->set_written(written);
    reply(proto_res, res);
}

static void error_put(status status, std::shared_ptr<tateyama::api::server::response> &res,
                      std::string *message = nullptr) {
    tateyama::proto::kvs::response::Response proto_res { };
    auto* put = proto_res.mutable_put();
    auto* error = put->mutable_error();
    auto alloc_detail = set_error(status, message, *error);
    reply(proto_res, res);
    if (alloc_detail) {
        (void)error->release_detail();
    }
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
    status status{};
    {
        std::unique_lock<std::mutex> lock{tx->transaction_mutex()};
        status = tx->put(table, record, opt);
    }
    switch (status) {
        case status::ok:
            success_put(1, res);
            break;
        case status::already_exists: // opt==create && didn't create
        case status::not_found: // opt==update && didn't update
            success_put(0, res);
            break;
        case status::err_not_implemented: {
            // TODO better message handling
            std::string msg{"table with secondary index not fully supported yet"};
            error_put(status, res, &msg);
            break;
        }
        default:
            error_put(status, res);
            break;
    }
}

/*
 * get
 */
static void success_get(tateyama::proto::kvs::response::Get_Success &success, std::shared_ptr<tateyama::api::server::response> &res) {
    tateyama::proto::kvs::response::Response proto_res { };
    auto* get = proto_res.mutable_get();
    get->set_allocated_success(&success);
    reply(proto_res, res);
    while (success.records_size() > 0) {
        (void)success.mutable_records()->ReleaseLast();
    }
    (void)get->release_success();
}

static void error_get(status status, std::shared_ptr<tateyama::api::server::response> &res) {
    tateyama::proto::kvs::response::Response proto_res { };
    auto* get = proto_res.mutable_get();
    auto* error = get->mutable_error();
    set_error(status, *error);
    reply(proto_res, res);
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
    auto &key = get.keys(0);
    tateyama::proto::kvs::data::Record record{};
    status status{};
    {
        std::unique_lock<std::mutex> lock{tx->transaction_mutex()};
        status = tx->get(table, key, record);
    }
    if (status != status::ok && status != status::not_found) {
        error_get(status, res);
        return;
    }
    tateyama::proto::kvs::response::Get_Success success{};
    if (status == status::ok) {
        success.mutable_records()->AddAllocated(&record);
    }
    success_get(success, res);
    while (success.records_size() > 0) {
        (void)success.mutable_records()->ReleaseLast();
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
    tateyama::proto::kvs::response::Response proto_res { };
    auto* remove = proto_res.mutable_remove();
    auto* success = remove->mutable_success();
    success->set_removed(removed);
    reply(proto_res, res);
}

static void error_remove(status status, std::shared_ptr<tateyama::api::server::response> &res,
                         std::string *message = nullptr) {
    tateyama::proto::kvs::response::Response proto_res { };
    auto* remove = proto_res.mutable_remove();
    auto* error = remove->mutable_error();
    auto alloc_detail = set_error(status, message, *error);
    reply(proto_res, res);
    if (alloc_detail) {
        (void)error->release_detail();
    }
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
    status status{};
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
        case status::err_not_implemented: {
            // TODO better message handling
            std::string msg{"table with secondary index not fully supported yet"};
            error_remove(status, res, &msg);
            break;
        }
        default:
            error_remove(status, res);
            break;
    }
}

/*
 * get_error_info
 */
static void has_error_get_error_info(tateyama::proto::kvs::response::Error &error,
                                     std::shared_ptr<tateyama::api::server::response> &res) {
    tateyama::proto::kvs::response::Response proto_res { };
    auto* getinfo = proto_res.mutable_get_error_info();
    getinfo->set_allocated_error(&error);
    reply(proto_res, res);
    (void)getinfo->release_error();
}

static void no_error_get_error_info(std::shared_ptr<tateyama::api::server::response> &res) {
    tateyama::proto::kvs::response::Response proto_res { };
    auto* getinfo = proto_res.mutable_get_error_info();
    auto* v = getinfo->mutable_error_not_found();
    (void) v;
    reply(proto_res, res);
}

static void error_get_error_info(status status, std::shared_ptr<tateyama::api::server::response> &res) {
    tateyama::proto::kvs::response::Response proto_res { };
    auto* getinfo = proto_res.mutable_get_error_info();
    auto* error = getinfo->mutable_error();
    set_error(status, *error);
    reply(proto_res, res);
}

void service::command_get_error_info(tateyama::proto::kvs::request::Request const &proto_req,
                                          std::shared_ptr<tateyama::api::server::response> &res) {
    auto &get_info = proto_req.get_error_info();
    auto &proto_handle = get_info.transaction_handle();
    auto tx = store_->find_transaction(proto_handle.system_id());
    if (tx == nullptr) {
        error_get_error_info(status::err_invalid_argument, res);
        return;
    }
    auto tx_error = tx->get_error_info();
    if (tx_error.code() != 0) {
        has_error_get_error_info(tx_error, res);
    } else {
        no_error_get_error_info(res);
    }
}

/*
 * dispose_transaction
 */
static void success_dispose_transaction(std::shared_ptr<tateyama::api::server::response> &res) {
    tateyama::proto::kvs::response::Response proto_res { };
    auto* dispose = proto_res.mutable_dispose_transaction();
    auto* v = dispose->mutable_success();
    (void) v;
    reply(proto_res, res);
}

static void error_dispose_transaction(status status, std::shared_ptr<tateyama::api::server::response> &res) {
    tateyama::proto::kvs::response::Response proto_res { };
    auto* dispose = proto_res.mutable_dispose_transaction();
    auto* error = dispose->mutable_error();
    set_error(status, *error);
    reply(proto_res, res);
}

void service::command_dispose_transaction(tateyama::proto::kvs::request::Request const &proto_req,
                                          std::shared_ptr<tateyama::api::server::response> &res) {
    auto &dispose = proto_req.dispose_transaction();
    auto &proto_handle = dispose.transaction_handle();
    auto tx = store_->find_transaction(proto_handle.system_id());
    if (tx == nullptr) {
        error_dispose_transaction(status::err_invalid_argument, res);
        return;
    }
    auto status = store_->dispose_transaction(proto_handle.system_id());
    if (status == status::ok) {
        success_dispose_transaction(res);
    } else {
        error_dispose_transaction(status, res);
    }
}

/*
 * service protocol handling
 */
bool service::operator()(std::shared_ptr<tateyama::api::server::request> req, // NOLINT
                              std::shared_ptr<tateyama::api::server::response> res) {
    tateyama::proto::kvs::request::Request proto_req { };
    res->session_id(req->session_id());
    auto s = req->payload();
    if (!proto_req.ParseFromArray(s.data(), static_cast<int>(s.size()))) {
        reply("parse error with request body", res);
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
            reply("not supported yet", res);
            break;
        }
        case tateyama::proto::kvs::request::Request::kBatch: {
            reply("not supported yet", res);
            break;
        }
        case tateyama::proto::kvs::request::Request::kGetErrorInfo: {
            command_get_error_info(proto_req, res);
            break;
        }
        case tateyama::proto::kvs::request::Request::kDisposeTransaction: {
            command_dispose_transaction(proto_req, res);
            break;
        }
        case tateyama::proto::kvs::request::Request::COMMAND_NOT_SET: {
            // NOTE: for transfer benchmark of empty message
            // see tsubakuro/modules/kvs/src/bench/java/com/tsurugidb/tsubakuro/kvs/bench/EmptyMessageBench.java
            reply("", res);
            break;
        }
        default:
            reply("invalid request code", res);
            break;
    }

    return true;
}

}
