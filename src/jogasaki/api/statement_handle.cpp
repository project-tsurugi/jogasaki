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
#include <cstdint>

#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/impl/prepared_statement.h>
#include <jogasaki/api/record_meta.h>
#include <jogasaki/api/statement_handle.h>

namespace jogasaki::api {

api::record_meta const* statement_handle::meta() const noexcept {
    return reinterpret_cast<impl::prepared_statement*>(body_)->meta();  //NOLINT
}

statement_handle::statement_handle(
    void* arg,
    void* db
) noexcept:
    body_(reinterpret_cast<std::uintptr_t>(arg)),  //NOLINT
    db_(reinterpret_cast<std::uintptr_t>(db))  //NOLINT
{}

statement_handle::statement_handle(
    std::uintptr_t arg,
    std::uintptr_t db
) noexcept:
    body_(arg),
    db_(db)
{}

std::uintptr_t statement_handle::get() const noexcept {
    return body_;
}

statement_handle::operator std::size_t() const noexcept {
    return reinterpret_cast<std::size_t>(body_);  //NOLINT
}

statement_handle::operator bool() const noexcept {
    return body_ != 0;
}

bool statement_handle::has_result_records() const noexcept {
    return reinterpret_cast<impl::prepared_statement*>(body_)->has_result_records();  //NOLINT
}

std::pair<api::impl::database*, std::shared_ptr<impl::prepared_statement>> extract_statement_body(std::uintptr_t db, std::uintptr_t stmt) {
    if(db == 0) return {};
    auto* dbp = reinterpret_cast<api::impl::database*>(db);  //NOLINT
    auto s = dbp->find_statement(statement_handle{stmt, db});
    return {dbp, std::move(s)};
}

std::uintptr_t statement_handle::db() const noexcept {
    return db_;
}

std::shared_ptr<impl::prepared_statement> get_statement(statement_handle arg) {
    auto [db, stmt] = extract_statement_body(arg.db(), arg.get());
    (void) db;
    return stmt;
}

}  // namespace jogasaki::api
