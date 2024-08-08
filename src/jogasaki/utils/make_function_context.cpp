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
#include "make_function_context.h"

#include <jogasaki/transaction_context.h>
#include <jogasaki/executor/process/impl/expression/function_evaluation_context.h>

namespace jogasaki::utils {

using jogasaki::executor::process::impl::expression::function_evaluation_context;

std::shared_ptr<function_evaluation_context> make_function_context(transaction_context const& tx) {
    auto ret = std::make_shared<function_evaluation_context>();
    ret->transaction_begin(tx.start_time());
    return ret;
}

static_assert(std::is_same_v<transaction_context::clock, function_evaluation_context::clock>);

}  // namespace jogasaki::utils
