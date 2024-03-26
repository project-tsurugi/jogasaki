/*
 * Copyright 2018-2023 Project Tsurugi.
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
#pragma once

#include <memory>

#include <tateyama/api/configuration.h>
#include <tateyama/api/server/request.h>
#include <tateyama/api/server/response.h>
#include <tateyama/framework/component_ids.h>
#include <tateyama/framework/environment.h>
#include <tateyama/framework/repository.h>
#include <tateyama/framework/service.h>
#include <tateyama/proto/kvs/data.pb.h>
#include <tateyama/proto/kvs/request.pb.h>
#include <tateyama/proto/kvs/response.pb.h>
#include <tateyama/proto/kvs/transaction.pb.h>

#include <jogasaki/api/kvsservice/store.h>

namespace jogasaki::api::kvsservice::impl {

class service {
public:
    service() = default;

    explicit service(std::shared_ptr<tateyama::api::configuration::whole> const &cfg,
                     jogasaki::api::kvsservice::store* store);

    bool operator()(std::shared_ptr<tateyama::api::server::request const> req,
                    std::shared_ptr<tateyama::api::server::response> res);

    bool start();

    bool shutdown(bool force = false);

private:
    void command_begin(tateyama::proto::kvs::request::Request const &proto_req,
                       std::shared_ptr<tateyama::api::server::response> &res);
    void command_commit(tateyama::proto::kvs::request::Request const &proto_req,
                        std::shared_ptr<tateyama::api::server::response> &res);
    void command_rollback(tateyama::proto::kvs::request::Request const &proto_req,
                          std::shared_ptr<tateyama::api::server::response> &res);
    void command_put(tateyama::proto::kvs::request::Request const &proto_req,
                     std::shared_ptr<tateyama::api::server::response> &res);
    void command_get(tateyama::proto::kvs::request::Request const &proto_req,
                     std::shared_ptr<tateyama::api::server::response> &res);
    void command_remove(tateyama::proto::kvs::request::Request const &proto_req,
                        std::shared_ptr<tateyama::api::server::response> &res);
    void command_get_error_info(tateyama::proto::kvs::request::Request const &proto_req,
                                     std::shared_ptr<tateyama::api::server::response> &res);
    void command_dispose_transaction(tateyama::proto::kvs::request::Request const &proto_req,
                                   std::shared_ptr<tateyama::api::server::response> &res);

    jogasaki::api::kvsservice::store* store_{};
};

}
