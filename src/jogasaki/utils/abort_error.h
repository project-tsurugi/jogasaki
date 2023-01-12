/*
 * Copyright 2018-2020 tsurugi project.
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
#pragma once

#include <yugawara/storage/configurable_provider.h>

#include <jogasaki/request_context.h>
#include <jogasaki/transaction_context.h>

namespace jogasaki::utils {

void set_abort_message(request_context& rctx, transaction_context& tx, yugawara::storage::configurable_provider const& tables);

std::shared_ptr<yugawara::storage::index const> find_storage(
    yugawara::storage::configurable_provider const& tables,
    std::string_view storage_name
);

}
