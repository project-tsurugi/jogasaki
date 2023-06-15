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

void service::command_begin(tateyama::proto::kvs::request::Request const &,
                                 std::shared_ptr<tateyama::api::server::response> const &) {
}

void service::command_commit(tateyama::proto::kvs::request::Request const&,
                                  std::shared_ptr<tateyama::api::server::response> const &) {
}

void service::command_rollback(tateyama::proto::kvs::request::Request const&,
                                    std::shared_ptr<tateyama::api::server::response> const &) {
}

void service::command_put(tateyama::proto::kvs::request::Request const &,
                               std::shared_ptr<tateyama::api::server::response> const &) {
}

void service::command_get(tateyama::proto::kvs::request::Request const &,
                               std::shared_ptr<tateyama::api::server::response> const &) {
}

void service::command_remove(tateyama::proto::kvs::request::Request const &,
                                  std::shared_ptr<tateyama::api::server::response> const &) {
}

bool service::operator()(std::shared_ptr<tateyama::api::server::request const> req,
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

bool service::start() {
    return true;
}

bool service::shutdown(bool) {
    return true;
}

}
