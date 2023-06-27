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

#define DUMMY_TX // FIXME just for test only
#ifdef DUMMY_TX
static std::shared_ptr<transaction> dummy_tx = std::make_shared<transaction>(nullptr);
#endif

status store::begin_transaction(transaction_option const &, std::shared_ptr<transaction>& tx) {
#ifndef DUMMY_TX
    // FIXME call sharksfin: transaction_begin
    sharksfin::TransactionControlHandle handle {};
    tx = std::make_shared<transaction>(handle);
    //
    decltype(transactions_)::accessor acc{};
    if (transactions_.insert(acc, tx->system_id())) {
        acc->second = tx;
    }
#else
    tx = dummy_tx;
#endif
    return status::ok;
}

std::shared_ptr<transaction> store::find_transaction(std::uint64_t
#ifndef DUMMY_TX
system_id
#endif
) {
#ifndef DUMMY_TX
    decltype(transactions_)::accessor acc{};
    if (transactions_.find(acc, system_id)) {
        return acc->second;
    }
    return nullptr;
#else
    return dummy_tx;
#endif
}

status store::dispose_transaction(std::uint64_t
#ifndef DUMMY_TX
system_id
#endif
) {
    // FIXME call sharksfin: transaction_dispose
#ifndef DUMMY_TX
    decltype(transactions_)::accessor acc{};
    if (transactions_.find(acc, system_id)) {
        transactions_.erase(acc);
    }
#endif
    return status::ok;
}
}
