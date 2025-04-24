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
#include "scan_parallel_enabled.h"

#include <jogasaki/executor/global.h>
#include <jogasaki/configuration.h>

namespace jogasaki::utils {

std::pair<bool, std::uint32_t> scan_parallel_enabled(transaction_context const& tctx) {
    auto rtx_parallel_scan_enabled = global::config_pool()->rtx_parallel_scan();
    auto scan_parallel_count = global::config_pool()->scan_default_parallel();
    auto& option= tctx.option();
    if (option && option->scan_parallel().has_value()) {
        scan_parallel_count = option->scan_parallel().value();
        rtx_parallel_scan_enabled = (scan_parallel_count > 0);
    }
    return {rtx_parallel_scan_enabled, scan_parallel_count};
}

} // namespace jogasaki::utils


