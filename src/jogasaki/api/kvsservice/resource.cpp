/*
 * Copyright 2018-2024 Project Tsurugi.
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
#include <ostream>
#include <vector>
#include <glog/logging.h>

#include <tateyama/framework/boot_mode.h>
#include <tateyama/framework/repository.h>

#include <jogasaki/api/kvsservice/resource.h>
#include <jogasaki/api/kvsservice/store.h>
#include <jogasaki/api/resource/bridge.h>

namespace jogasaki::api::kvsservice {

namespace framework = tateyama::framework;

resource::resource() = default;

framework::component::id_type resource::id() const noexcept {
    return tag;
}

bool resource::setup(framework::environment&) {
    return true;
}

bool resource::start(framework::environment& env) {
    // on maintenance/quiescent mode, sql resource exists, but does nothing.
    // see setup() in src/jogasaki/api/resource/bridge.cpp
    if(env.mode() == framework::boot_mode::maintenance_standalone ||
       env.mode() == framework::boot_mode::maintenance_server || env.mode() == framework::boot_mode::quiescent_server) {
        return true;
    }
    if (! store_) {
        auto bridge = env.resource_repository().find<jogasaki::api::resource::bridge>();
        if(! bridge) {
            LOG(ERROR) << "failed to find jogasaki resource bridge";
            return false;
        }
        store_ = std::make_unique<jogasaki::api::kvsservice::store>(bridge);
    }
    return true;
}

bool resource::shutdown(framework::environment&) {
    return true;
}

resource::~resource() = default;

store* resource::store() const noexcept {
    if (store_ != nullptr) {
        return store_.get();
    }
    return nullptr;
}

std::string_view resource::label() const noexcept {
    return component_label;
}

}
