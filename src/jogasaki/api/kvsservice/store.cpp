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

#include <ostream>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <glog/logging.h>

#include <takatori/util/exception.h>
#include <sharksfin/StatusCode.h>
#include <sharksfin/TransactionOptions.h>
#include <sharksfin/api.h>

#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/kvsservice/store.h>
#include <jogasaki/api/kvsservice/transaction.h>
#include <jogasaki/api/kvsservice/transaction_option.h>
#include <jogasaki/api/kvsservice/transaction_type.h>
#include <jogasaki/api/resource/bridge.h>
#include <jogasaki/kvs/database.h>

#include "convert.h"

using takatori::util::throw_exception;

namespace jogasaki::api::kvsservice {

constexpr static std::string_view log_location_prefix = "/:jogasaki:api:kvsservice:store ";

store::store(std::shared_ptr<jogasaki::api::resource::bridge> const& bridge) :
    db_(bridge->database()), db_handle_(dynamic_cast<jogasaki::api::impl::database*>(db_)->kvs_db()->handle()) {
}

store::~store() {
    for (const auto & pair : transactions_) {
        auto s = pair.second->dispose();
        if (s != status::ok) {
            LOG(ERROR) << "failed dispose kvs transaction";
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
    return {type, wps};
}

status store::begin_transaction(transaction_option const &option, std::shared_ptr<transaction>& tx) {
    sharksfin::TransactionControlHandle handle{};
    auto options = convert(option);
    auto state = sharksfin::transaction_begin(db_handle_, options, &handle);
    if (state != sharksfin::StatusCode::OK) {
        return convert(state);
    }
    tx = std::make_shared<transaction>(db_, handle);
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
        auto s = acc->second->dispose();
        if (s != status::ok) {
            LOG(ERROR) << log_location_prefix << "transaction::dispose() failed: "<< s;
        }
        transactions_.erase(acc);
    }
    return status::ok;
}
}
