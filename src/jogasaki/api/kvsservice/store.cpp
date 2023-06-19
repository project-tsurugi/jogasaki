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

namespace jogasaki::api::kvsservice {

store::store(std::shared_ptr<jogasaki::api::resource::bridge> const& bridge) :
    db_(dynamic_cast<jogasaki::api::impl::database*>(bridge->database())->kvs_db()->handle()){
}

status store::transaction_begin(transaction_option const&, std::shared_ptr<transaction>& tx) {
    // FIXME call sharksfin: transaction_begin
    sharksfin::TransactionControlHandle handle {};
    tx = std::make_shared<transaction>(handle);
    //
    decltype(transactions_)::accessor acc{};
    if (transactions_.insert(acc, tx->system_id())) {
        acc->second = tx;
    }
    return status::ok;
}

std::shared_ptr<transaction> store::transaction_find(std::uint64_t system_id) {
    decltype(transactions_)::accessor acc{};
    if (transactions_.find(acc, system_id)) {
        return acc->second;
    }
    return nullptr;
}

status store::transaction_dispose(std::uint64_t system_id) {
    // FIXME call sharksfin: transaction_dispose
    //
    decltype(transactions_)::accessor acc{};
    if (transactions_.find(acc, system_id)) {
        transactions_.erase(acc);
    }
    return status::ok;
}
}
