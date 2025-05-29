/*
 * Copyright 2018-2025 Project Tsurugi.
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
#include "transaction_store.h"

#include <jogasaki/api/impl/database.h>
#include <jogasaki/executor/global.h>

namespace jogasaki::api::impl {

transaction_store::transaction_store(std::size_t session_id) noexcept :
    session_id_(session_id)
{}

std::shared_ptr<transaction_context> transaction_store::lookup(transaction_handle handle) {
    decltype(transactions_)::const_accessor acc;
    if (transactions_.find(acc, handle)) {
      return acc->second;
    }
    return nullptr;
}

bool transaction_store::put(transaction_handle handle, std::shared_ptr<transaction_context> arg) {
    return transactions_.emplace(handle, std::move(arg));
}

bool transaction_store::remove(transaction_handle handle) {
    return transactions_.erase(handle);
}

void transaction_store::dispose() {
    transactions_.clear();
    global::database_impl()->remove_transaction_store(session_id_);
}

std::size_t transaction_store::size() const noexcept {
    return transactions_.size();
}

std::size_t transaction_store::session_id() const noexcept {
    return session_id_;
}

} // namespace jogasaki::api::impl
