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

#include <memory>

#include <jogasaki/api/database.h>
#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/api/transaction_option.h>
#include <jogasaki/api/impl/database.h>

namespace jogasaki::utils {

std::shared_ptr<api::transaction_handle> create_transaction(
    api::database& db,
    bool readonly,
    bool is_long,
    std::vector<std::string> const& write_preserves = {}
);

std::shared_ptr<api::transaction_handle> create_transaction(
    api::database& db,
    bool force_ltx = false
);

}

