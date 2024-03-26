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
#include "create_commit_option.h"

#include <memory>

#include <jogasaki/api/commit_option.h>

namespace jogasaki::utils {

std::unique_ptr<api::commit_option> g_commit_option{std::make_unique<api::commit_option>()};

void set_global_commit_option(api::commit_option const& opt) {
    g_commit_option = std::make_unique<api::commit_option>(opt);
}

api::commit_option* get_global_commit_option() {
    return g_commit_option.get();
}
}

