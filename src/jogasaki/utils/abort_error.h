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
#pragma once

#include <memory>
#include <string>
#include <string_view>

#include <yugawara/storage/configurable_provider.h>
#include <yugawara/storage/index.h>

#include <jogasaki/request_context.h>
#include <jogasaki/transaction_context.h>

namespace jogasaki::utils {

std::string create_abort_message(request_context const& rctx);

std::shared_ptr<yugawara::storage::index const> find_storage(
    yugawara::storage::configurable_provider const& tables,
    std::string_view storage_name
);

std::string transaction_id_str(transaction_context& tx);

}

