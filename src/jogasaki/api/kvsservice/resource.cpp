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
#include <jogasaki/api/kvsservice/resource.h>

namespace jogasaki::api::kvsservice {

namespace framework = tateyama::framework;

resource::resource() = default;

framework::component::id_type resource::id() const noexcept {
    return tag;
}

bool resource::setup(framework::environment&) {
    return true;
}

bool resource::start(framework::environment&) {
    return true;
}

bool resource::shutdown(framework::environment&) {
    return true;
}

resource::~resource() {
}

std::string_view resource::label() const noexcept {
    return component_label;
}

}
