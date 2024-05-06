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
#include "cancel_request.h"

#include <iomanip>
#include <cstddef>
#include <cstdint>
#include <cstdlib>

#include <jogasaki/executor/global.h>
#include <jogasaki/configuration.h>

namespace jogasaki::utils {

bool request_cancel_enabled(request_cancel_kind kind) noexcept {
    auto& c = global::config_pool();
    if(! c || ! c->req_cancel_config()) {
        return true;
    }
    return c->req_cancel_config()->is_enabled(kind);
}

}  // namespace jogasaki::utils
