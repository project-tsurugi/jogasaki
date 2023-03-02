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
#include <tateyama/framework/component_ids.h>
#include <tateyama/diagnostic/resource/diagnostic_resource.h>

#include <jogasaki/api/impl/service.h>
#include <jogasaki/api/resource/bridge.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/logging.h>

namespace jogasaki::api::service {

using tateyama::api::server::request;
using tateyama::api::server::response;
namespace framework = tateyama::framework;

bridge::bridge() = default;

framework::component::id_type bridge::id() const noexcept {
    return tag;
}

bool bridge::setup(framework::environment& env) {
    // on maintenance/quiescent mode, sql service exists, but does nothing and returns error on sql request.
    if(env.mode() == framework::boot_mode::maintenance_standalone ||
        env.mode() == framework::boot_mode::maintenance_server ||
        env.mode() == framework::boot_mode::quiescent_server) {
        return true;
    }
    if (core_) return true;
    auto br = env.resource_repository().find<resource::bridge>();
    if(! br) {
        LOG(ERROR) << "setup error";
        return false;
    }
    core_ = std::make_unique<jogasaki::api::impl::service>(env.configuration(), br->database());
    return true;
}

bool bridge::start(framework::environment& env) {
    // on maintenance/quiescent mode, sql service exists, but does nothing and returns error on sql request.
    if(env.mode() == framework::boot_mode::maintenance_standalone ||
        env.mode() == framework::boot_mode::maintenance_server ||
        env.mode() == framework::boot_mode::quiescent_server) {
        quiescent_or_maintenance_ = true;
        return true;
    }
    auto db = core_->database();
    auto diagnostic_resource = env.resource_repository().find<tateyama::diagnostic::resource::diagnostic_resource>();
    diagnostic_resource->add_print_callback("jogasaki", [db](std::ostream& os) {
        db->print_diagnostic(os);
    });
    return core_->start();
}

bool bridge::shutdown(framework::environment& env) {
    if(core_) {
        auto ret = core_->shutdown();
        deactivated_ = ret;
        auto diagnostic_resource = env.resource_repository().find<tateyama::diagnostic::resource::diagnostic_resource>();
        diagnostic_resource->remove_print_callback("jogasaki");
        return ret;
    }
    return true;
}

bool bridge::operator()(std::shared_ptr<request> req, std::shared_ptr<response> res) {
    if(quiescent_or_maintenance_) {
        LOG(ERROR) << "service is running on quiescent or maintenance mode - no request is allowed";
        return false;
    }
    return (*core_)(std::move(req), std::move(res));
}

bridge::~bridge() {
    if(core_ && ! deactivated_) {
        core_->shutdown(true);
    }
}

jogasaki::api::database* bridge::database() const noexcept {
    if(core_) {
        return core_->database();
    }
    return {};
}

}

