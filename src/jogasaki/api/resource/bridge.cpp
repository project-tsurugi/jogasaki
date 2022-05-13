/*
 * Copyright 2018-2022 tsurugi project.
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
#include <jogasaki/api/resource/bridge.h>

#include <functional>
#include <memory>
#include <type_traits>

#include <tateyama/framework/service.h>
#include <tateyama/framework/repository.h>
#include <tateyama/api/server/request.h>
#include <tateyama/api/server/response.h>
#include <tateyama/framework/environment.h>
#include <tateyama/framework/component_ids.h>
#include <tateyama/framework/transactional_kvs_resource.h>

#include <jogasaki/api/impl/service.h>

namespace jogasaki::api::resource {

namespace framework = tateyama::framework;

bridge::bridge() = default;

framework::component::id_type bridge::id() const noexcept {
    return tag;
}

bool bridge::setup(framework::environment& env) {
    if (db_) return true;
    auto kvs = env.resource_repository().find<framework::transactional_kvs_resource>();
    if(! kvs) {
        LOG(ERROR) << "failed to find transactional kvs";
        return false;
    }
    db_ = jogasaki::api::create_database(convert_config(*env.configuration()), kvs->core_object());
    return true;
}

bool bridge::start(framework::environment&) {
    return db_->start() == status::ok;
}

bool bridge::shutdown(framework::environment&) {
    auto ret = db_->stop() == status::ok;
    deactivated_ = ret;
    return ret;
}

bridge::~bridge() {
    if(db_ && ! deactivated_) {
        db_->stop();
    }
}

jogasaki::api::database* bridge::database() const noexcept {
    return db_.get();
}

std::shared_ptr<jogasaki::configuration> bridge::convert_config(tateyama::api::configuration::whole& cfg) {
    auto ret = std::make_shared<jogasaki::configuration>();

    auto jogasaki_config = cfg.get_section("sql");
    if (jogasaki_config == nullptr) {
        LOG(ERROR) << "cannot find sql section in the configuration";
        return ret;
    }

    std::size_t thread_pool_size{};
    if (jogasaki_config->get<>("thread_pool_size", thread_pool_size)) {
        ret->thread_pool_size(thread_pool_size);
    }
    bool lazy_worker{};
    if (jogasaki_config->get<>("lazy_worker", lazy_worker)) {
        ret->lazy_worker(lazy_worker);
    }

    // data_store
    auto data_store_config = cfg.get_section("data_store");
    if (data_store_config == nullptr) {
        LOG(ERROR) << "cannot find data_store section in the configuration";
        return ret;
    }
    std::string log_location{};
    if (data_store_config->get<std::string>("log_location", log_location)) {
        ret->db_location(log_location);
    }
    return ret;
}
}

