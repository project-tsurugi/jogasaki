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
#pragma once

#include <memory>

#include <jogasaki/api/database.h>
#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/api/transaction_option.h>
#include <jogasaki/api/impl/database.h>

namespace jogasaki::utils {

struct create_tx_option {
    bool force_ltx = false;
    bool force_occ = false;
};

void set_global_tx_option(create_tx_option const& opt);

create_tx_option* get_global_tx_option();

std::shared_ptr<api::transaction_handle> create_transaction(api::database& db, api::transaction_option options);

std::shared_ptr<api::transaction_handle> create_transaction(
    api::database& db,
    bool readonly,
    bool is_long,
    std::vector<std::string> const& write_preserves = {},
    std::vector<std::string> const& read_areas_inclusive = {},
    std::vector<std::string> const& read_areas_exclusive = {},
    std::string_view label = {}
);

std::shared_ptr<api::transaction_handle> create_transaction(
    api::database& db,
    bool force_ltx = false
);

}

