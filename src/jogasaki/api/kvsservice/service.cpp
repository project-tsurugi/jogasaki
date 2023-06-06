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
#include <jogasaki/api/kvsservice/service.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/logging.h>
#include "impl/service.h"

namespace jogasaki::api::kvsservice {

using tateyama::api::server::request;
using tateyama::api::server::response;
namespace framework = tateyama::framework;

service::service() = default;

framework::component::id_type service::id() const noexcept {
    return tag;
}

bool service::setup(framework::environment& env) {
    if (core_) return true;
    // auto rsc = env.resource_repository().find<jogasaki::api::kvsservice::resource>();
    // FIXME
    core_ = std::make_unique<jogasaki::api::kvsservice::impl::service>(env.configuration(), nullptr);
    return true;
}

bool service::start(framework::environment&) {
    return core_->start();
}

bool service::shutdown(framework::environment&) {
    if(core_) {
        auto ret = core_->shutdown();
        deactivated_ = ret;
        return ret;
    }
    return true;
}

bool service::operator()(std::shared_ptr<request> req, std::shared_ptr<response> res) {
    return (*core_)(std::move(req), std::move(res));
}

service::~service() {
    if(core_ && ! deactivated_) {
        core_->shutdown(true);
    }
    VLOG(log_info) << "/:tateyama:lifecycle:component:<dtor> " << component_label;
}

std::string_view service::label() const noexcept {
    return component_label;
}

}
