/*
 * Copyright 2018-2023 tsurugi project.
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

#include <jogasaki/api/kvsservice/store.h>
#include <jogasaki/api/impl/database.h>
#include <sharksfin/api.h>

#include "convert.h"

#include <iostream>

namespace jogasaki::api::kvsservice {

store::store(std::shared_ptr<jogasaki::api::resource::bridge> const& bridge) {
//    db_(dynamic_cast<jogasaki::api::impl::database*>(bridge->database())->kvs_db()->handle()){
    if (bridge.get() == nullptr) {
        throw_exception(std::logic_error{"jogasaki::api::resource::bridge is null"});
    }
    auto db = dynamic_cast<jogasaki::api::impl::database*>(bridge->database());
    if (db == nullptr) {
        throw_exception(std::logic_error{"jogasaki::api::resource::bridge->database() is null"});
    }
    auto kvs = db->kvs_db();
    if (kvs == nullptr) {
        throw_exception(std::logic_error{"kvs is null"});
    }
    db_ = kvs->handle();
    if (db_ == nullptr) {
        throw_exception(std::logic_error{"kvs->handle() is null"});
    }
    // FIXME just for debug
    std::vector<std::string> names{};
    auto status = sharksfin::storage_list(db_, names);
    if (status == sharksfin::StatusCode::OK) {
        for (auto &name : names) {
            std::cout << name << std::endl;
        }
    }
}

static sharksfin::TransactionOptions::TransactionType convert(transaction_type type) {
    switch (type) {
        case transaction_type::unspecified:
            return sharksfin::TransactionOptions::TransactionType::SHORT;
        case transaction_type::occ:
            return sharksfin::TransactionOptions::TransactionType::SHORT;
        case transaction_type::ltx:
            return sharksfin::TransactionOptions::TransactionType::LONG;
        case transaction_type::read_only:
            return sharksfin::TransactionOptions::TransactionType::READ_ONLY;
        default:
            throw_exception(std::logic_error{"unknown transaction_type"});
    }
}
static sharksfin::TransactionOptions convert(transaction_option const &option) {
    auto type = convert(option.type());
    sharksfin::TransactionOptions::WritePreserves wps{}; // FIXME
    return sharksfin::TransactionOptions(type, wps);
}

status store::begin_transaction(transaction_option const &option, std::shared_ptr<transaction>& tx) {
    sharksfin::TransactionControlHandle handle{};
    auto options = convert(option);
    auto state = sharksfin::transaction_begin(db_, options, &handle);
    if (state != sharksfin::StatusCode::OK) {
        return convert(state);
    }
    tx = std::make_shared<transaction>(handle);
    //
    decltype(transactions_)::accessor acc{};
    if (transactions_.insert(acc, tx->system_id())) {
        acc->second = tx;
    }
    return status::ok;
}

std::shared_ptr<transaction> store::find_transaction(std::uint64_t system_id) {
    decltype(transactions_)::accessor acc{};
    if (transactions_.find(acc, system_id)) {
        return acc->second;
    }
    return nullptr;
}

status store::dispose_transaction(std::uint64_t system_id) {
    decltype(transactions_)::accessor acc{};
    if (transactions_.find(acc, system_id)) {
        transactions_.erase(acc);
    }
    return status::ok;
}
}
