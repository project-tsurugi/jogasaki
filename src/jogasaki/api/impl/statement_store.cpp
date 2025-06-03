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
#include "statement_store.h"

#include <jogasaki/api/impl/database.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>

namespace jogasaki::api::impl {

statement_store::statement_store(std::size_t session_id) noexcept :
    session_id_(session_id)
{}

std::shared_ptr<prepared_statement> statement_store::lookup(statement_handle handle) {
    decltype(statements_)::const_accessor acc;
    if (statements_.find(acc, handle)) {
        return acc->second;
    }
    return nullptr;
}

bool statement_store::put(statement_handle handle, std::shared_ptr<prepared_statement> arg) {
    return statements_.emplace(handle, std::move(arg));
}

bool statement_store::remove(statement_handle handle) {
    return statements_.erase(handle);
}

void statement_store::dispose() {
    if(VLOG_IS_ON(log_debug)) {
        for(auto&& t: statements_) {
            VLOG_LP(log_debug) << "disposing prepared statement:" << std::hex << t.second.get() << " sql:\""
                               << t.second->body()->sql_text() << "\"";
        }
    }
    statements_.clear();
    global::database_impl()->remove_statement_store(session_id_);
}

std::size_t statement_store::size() const noexcept {
    return statements_.size();
}

std::size_t statement_store::session_id() const noexcept {
    return session_id_;
}

} // namespace jogasaki::api::impl
