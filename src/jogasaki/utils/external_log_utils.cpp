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
#include "external_log_utils.h"

#include <cstdlib>
#include <memory>

#include <jogasaki/external_log/events.h>
#include <jogasaki/kvs/transaction_option.h>
#include <jogasaki/transaction_context.h>

namespace jogasaki::utils {

std::int64_t tx_type_from(transaction_context const& tx) {
    auto& opt = tx.option();
    if(! opt) {
        return external_log::tx_type_value::unknown;
    }
    if (opt->readonly()) {
        return external_log::tx_type_value::rtx;
    } else if (opt->is_long()) {
        return external_log::tx_type_value::ltx;
    }
    return external_log::tx_type_value::occ;
}

std::int64_t result_from(status st) {
    return st == status::ok ? external_log::result_value::success : external_log::result_value::fail;
}

}  // namespace jogasaki::utils
