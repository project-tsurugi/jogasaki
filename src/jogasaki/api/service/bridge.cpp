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
#include <jogasaki/api/service/bridge.h>

#include <functional>
#include <memory>
#include <type_traits>

#include <tateyama/framework/service.h>
#include <tateyama/framework/repository.h>
#include <tateyama/api/server/request.h>
#include <tateyama/api/server/response.h>
#include <tateyama/framework/environment.h>
#include <tateyama/framework/ids.h>

#include <tateyama/datastore/service/core.h>
#include <tateyama/datastore/resource/core.h>

#include <jogasaki/api/impl/service.h>

namespace jogasaki::api::service {

using tateyama::api::server::request;
using tateyama::api::server::response;
namespace framework = tateyama::framework;

bridge::bridge() = default;

framework::component::id_type bridge::id() const noexcept {
    return tag;
}

void bridge::setup(framework::environment& env) {
    core_ = std::make_unique<jogasaki::api::impl::service>(env.configuration());
}

void bridge::start(framework::environment&) {
    core_->start();
}

void bridge::shutdown(framework::environment&) {
    core_->shutdown();
}

void bridge::operator()(std::shared_ptr<request> req, std::shared_ptr<response> res) {
    (*core_)(std::move(req), std::move(res));
}

bridge::~bridge() {
    if(core_ && ! deactivated_) {
        core_->shutdown(true);
    }
}

jogasaki::api::impl::service* bridge::core_object() const noexcept {
    return core_.get();
}

}

