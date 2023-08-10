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
#include <jogasaki/api/transaction_handle.h>

#include <utility>
#include <memory>

#include <jogasaki/api/impl/database.h>
#include <jogasaki/executor/executor.h>

namespace jogasaki::api {

transaction_handle::transaction_handle(
    std::uintptr_t arg,
    std::uintptr_t db
) noexcept:
    body_(arg),
    db_(db)
{}

std::uintptr_t transaction_handle::get() const noexcept {
    return body_;
}

transaction_handle::operator std::size_t() const noexcept {
    return reinterpret_cast<std::size_t>(body_);  //NOLINT
}

transaction_handle::operator bool() const noexcept {
    return body_ != 0;
}

transaction_context* tx(std::uintptr_t arg) {
    return reinterpret_cast<transaction_context*>(arg);  //NOLINT
}

std::pair<api::impl::database*, std::shared_ptr<transaction_context>> cast(std::uintptr_t db, std::uintptr_t tx) {
    if(! db) return {};
    auto* dbp = reinterpret_cast<api::impl::database*>(db);  //NOLINT
    auto t = dbp->find_transaction(transaction_handle{tx, db});
    return {dbp, std::move(t)};
}

status transaction_handle::commit() {  //NOLINT(readability-make-member-function-const)
    auto [db, tx] = cast(db_, body_);
    if(! tx) return status::err_invalid_argument;
    return executor::commit(*db, tx);
}

void transaction_handle::commit_async(callback on_completion) {  //NOLINT(readability-make-member-function-const)
    auto [db, tx] = cast(db_, body_);
    if(! tx) {
        on_completion(status::err_invalid_argument, "invalid tx handle");
        return;
    }
    executor::commit_async(*db, tx, std::move(on_completion));
}
status transaction_handle::abort() {  //NOLINT(readability-make-member-function-const)
    auto [db, tx] = cast(db_, body_);
    if(! tx) return status::err_invalid_argument;
    (void) db;
    return executor::abort(tx);
}

status transaction_handle::execute(executable_statement& statement) {  //NOLINT(readability-make-member-function-const)
    auto [db, tx] = cast(db_, body_);
    if(! tx) return status::err_invalid_argument;
    std::unique_ptr<api::result_set> result{};
    return executor::execute(*db, tx, statement, result);
}

status transaction_handle::execute(executable_statement& statement, std::unique_ptr<result_set>& result) {  //NOLINT(readability-make-member-function-const)
    auto [db, tx] = cast(db_, body_);
    if(! tx) return status::err_invalid_argument;
    return executor::execute(*db, tx, statement, result);
}

status transaction_handle::execute( //NOLINT
    api::statement_handle prepared,
    std::shared_ptr<api::parameter_set> parameters,
    std::unique_ptr<result_set>& result
) {
    auto [db, tx] = cast(db_, body_);
    if(! tx) return status::err_invalid_argument;
    return executor::execute(*db, tx, prepared, std::move(parameters), result);
}

bool transaction_handle::execute_async(
    maybe_shared_ptr<executable_statement> const& statement,  //NOLINT(readability-make-member-function-const)
    transaction_handle::callback on_completion
) {
    auto [db, tx] = cast(db_, body_);
    return executor::execute_async(
        *db,
        tx,
        statement,
        nullptr,
        std::move(on_completion)
    );
}

bool transaction_handle::execute_async(
    maybe_shared_ptr<executable_statement> const& statement,  //NOLINT(readability-make-member-function-const)
    maybe_shared_ptr<data_channel> const& channel,
    transaction_handle::callback on_completion
) {
    auto [db, tx] = cast(db_, body_);
    if(! tx) {
        on_completion(status::err_invalid_argument, "invalid tx handle");
        return true;
    }
    return executor::execute_async(
        *db,
        tx,
        statement,
        channel,
        std::move(on_completion)
    );
}

transaction_handle::transaction_handle(
    void* arg,
    void* db
) noexcept:
    body_(reinterpret_cast<std::uintptr_t>(arg)),  //NOLINT
    db_(reinterpret_cast<std::uintptr_t>(db))  //NOLINT
{}

bool transaction_handle::is_ready() const {
    return tx(body_)->is_ready();
}

std::string_view transaction_handle::transaction_id() const noexcept {
    return tx(body_)->transaction_id();
}

}
