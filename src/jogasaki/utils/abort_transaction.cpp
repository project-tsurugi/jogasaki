/*
 * Copyright 2018-2025 Project Tsurugi.
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
#include "abort_transaction.h"

#include <memory>
#include <stdexcept>

#include <takatori/util/exception.h>

#include <jogasaki/status.h>
#include <jogasaki/transaction_context.h>

namespace jogasaki::utils {

using takatori::util::throw_exception;

void abort_transaction(kvs::transaction& tx) {
    if (auto res = tx.abort_transaction(); res != status::ok) {
        throw_exception(std::logic_error{"abort failed unexpectedly"});
    }
}

void abort_transaction(transaction_context& tx) {
    abort_transaction(*tx.object());
}

}  // namespace jogasaki::utils
