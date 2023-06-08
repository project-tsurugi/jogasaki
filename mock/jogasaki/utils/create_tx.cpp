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
#include "create_tx.h"

#include <memory>

#include <jogasaki/api/database.h>
#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/api/transaction_option.h>
#include <jogasaki/api/impl/database.h>

namespace jogasaki::utils {

std::shared_ptr<api::transaction_handle>
create_transaction(api::database& db, bool readonly, bool is_long,
    std::vector<std::string> const& write_preserves,
    std::vector<std::string> const& read_areas_inclusive,
    std::vector<std::string> const& read_areas_exclusive,
    std::string_view label
) {
    auto p = std::addressof(db);
    auto tx = std::shared_ptr<api::transaction_handle>{
        new api::transaction_handle(),
        [p](api::transaction_handle* arg) {
            if (arg != nullptr && *arg) {
                p->destroy_transaction(*arg);
                delete arg;  //NOLINT
            }
        }
    };
    if(auto rc = db.create_transaction(*tx, api::transaction_option{
            readonly,
            is_long,
            write_preserves,
            label,
            read_areas_inclusive,
            read_areas_exclusive
            }
        ); rc != status::ok) {
        return {};
    }
    return tx;
}

std::unique_ptr<create_tx_option> g_tx_option{};

std::shared_ptr<api::transaction_handle> create_transaction(
    api::database& db,
    bool force_ltx
) {
    if(g_tx_option && g_tx_option->force_occ) {
        return create_transaction(db, false, false);
    }
    // until LTX build becomes stables, test mainly with LTX //TODO
    (void) force_ltx;
    auto& impl = jogasaki::api::impl::get_impl(db);
    std::vector<std::string> wp{};
    impl.tables()->each_relation([&](std::string_view, std::shared_ptr<yugawara::storage::relation const> const& entry) {
        wp.emplace_back(entry->simple_name());
    });
    return create_transaction(db, false, true, wp);
}

void set_global_tx_option(create_tx_option const& opt) {
    g_tx_option = std::make_unique<create_tx_option>(opt);
}

create_tx_option* get_global_tx_option() {
    return g_tx_option.get();
}

}

