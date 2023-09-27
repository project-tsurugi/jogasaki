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

#include <jogasaki/api/transaction_handle_internal.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/impl/error_info.h>
#include <jogasaki/executor/executor.h>
#include <jogasaki/error/error_info_factory.h>

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
    if(db == 0) return {};
    auto* dbp = reinterpret_cast<api::impl::database*>(db);  //NOLINT
    auto t = dbp->find_transaction(transaction_handle{tx, db});
    return {dbp, std::move(t)};
}

status transaction_handle::commit(api::commit_option option) {  //NOLINT(readability-make-member-function-const, performance-unnecessary-value-param)
    auto [db, tx] = cast(db_, body_);
    if(! tx) return status::err_invalid_argument;
    return executor::commit(*db, tx, option);
}

void transaction_handle::commit_async(callback on_completion) {  //NOLINT(readability-make-member-function-const)
    commit_async([on_completion=std::move(on_completion)](status st, std::shared_ptr<api::error_info> info) {  //NOLINT(performance-unnecessary-value-param)
        on_completion(st, (info ? info->message() : ""));
    });
}

void transaction_handle::commit_async(error_info_callback on_completion, commit_option opt) {  //NOLINT(readability-make-member-function-const,performance-unnecessary-value-param)
    auto [db, tx] = cast(db_, body_);
    if(! tx) {
        auto res = status::err_invalid_argument;
        on_completion(res,
            api::impl::error_info::create(
                create_error_info(error_code::transaction_not_found_exception, "invalid tx handle", res)
            )
        );
        return;
    }
    executor::commit_async(*db, tx, [on_completion](status st, std::shared_ptr<error::error_info> info) {
        on_completion(st, api::impl::error_info::create(std::move(info)));
    }, opt);
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
    std::shared_ptr<error::error_info> info{};
    return executor::execute(*db, tx, statement, result, info);
}

status transaction_handle::execute(executable_statement& statement, std::unique_ptr<result_set>& result) {  //NOLINT(readability-make-member-function-const)
    auto [db, tx] = cast(db_, body_);
    if(! tx) return status::err_invalid_argument;
    std::shared_ptr<error::error_info> info{};
    return executor::execute(*db, tx, statement, result, info);
}

status transaction_handle::execute( //NOLINT
    api::statement_handle prepared,
    std::shared_ptr<api::parameter_set> parameters,
    std::unique_ptr<result_set>& result
) {
    auto [db, tx] = cast(db_, body_);
    if(! tx) return status::err_invalid_argument;
    std::shared_ptr<error::error_info> info{};
    return executor::execute(*db, tx, prepared, std::move(parameters), result, info);
}

bool transaction_handle::execute_async(   //NOLINT(readability-make-member-function-const)
    maybe_shared_ptr<executable_statement> const& statement,
    transaction_handle::callback on_completion
) {
    return execute_async(statement, [on_completion=std::move(on_completion)](status st, std::shared_ptr<api::error_info> info){  //NOLINT(performance-unnecessary-value-param)
        on_completion(st, (info ? info->message() : ""));
    });
}

bool transaction_handle::execute_async(   //NOLINT(readability-make-member-function-const)
    maybe_shared_ptr<executable_statement> const& statement,
    transaction_handle::error_info_callback on_completion  //NOLINT(performance-unnecessary-value-param)
) {
    auto [db, tx] = cast(db_, body_);
    if(! tx) {
        auto res = status::err_invalid_argument;
        on_completion(res,
            api::impl::error_info::create(
                create_error_info(error_code::transaction_not_found_exception, "invalid tx handle", res)
            )
        );
        return true;
    }
    return executor::execute_async(
        *db,
        tx,
        statement,
        nullptr,
        [on_completion](status st, std::shared_ptr<error::error_info> info) {
            on_completion(st, api::impl::error_info::create(std::move(info)));
        }
    );
}

bool transaction_handle::execute_async(  //NOLINT(readability-make-member-function-const)
    maybe_shared_ptr<executable_statement> const& statement,
    maybe_shared_ptr<data_channel> const& channel,
    transaction_handle::callback on_completion
) {
    return execute_async(statement, channel, [on_completion=std::move(on_completion)](status st, std::shared_ptr<api::error_info> info) {  //NOLINT(performance-unnecessary-value-param)
        on_completion(st, (info ? info->message() : ""));
    });
}

bool transaction_handle::execute_async(  //NOLINT(readability-make-member-function-const)
    maybe_shared_ptr<executable_statement> const& statement,
    maybe_shared_ptr<data_channel> const& channel,
    transaction_handle::error_info_callback on_completion  //NOLINT(performance-unnecessary-value-param)
) {
    auto [db, tx] = cast(db_, body_);
    if(! tx) {
        auto res = status::err_invalid_argument;
        on_completion(res,
            api::impl::error_info::create(
                create_error_info(error_code::transaction_not_found_exception, "invalid tx handle", res)
            )
        );
        return true;
    }
    return executor::execute_async(
        *db,
        tx,
        statement,
        channel,
        [on_completion](status st, std::shared_ptr<error::error_info> info) {
            on_completion(st, api::impl::error_info::create(std::move(info)));
        }
    );
}

transaction_handle::transaction_handle(
    void* arg,
    void* db
) noexcept:
    body_(reinterpret_cast<std::uintptr_t>(arg)),  //NOLINT
    db_(reinterpret_cast<std::uintptr_t>(db))  //NOLINT
{}

bool transaction_handle::is_ready_unchecked() const {
    return tx(body_)->is_ready();
}

std::string_view transaction_handle::transaction_id_unchecked() const noexcept {
    return tx(body_)->transaction_id();
}

std::string_view transaction_handle::transaction_id() const noexcept {
    auto [db, tx] = cast(db_, body_);
    (void) db;
    if(! tx) {
        return {};
    }
    return tx->transaction_id();
}

status transaction_handle::error_info(std::shared_ptr<api::error_info>& out) const noexcept {
    out = {};
    auto [db, tx] = cast(db_, body_);
    if(! tx) return status::err_invalid_argument;
    if(tx->error_info()) {
        out = api::impl::error_info::create(tx->error_info());
    }
    return status::ok;
}

std::uintptr_t transaction_handle::db() const noexcept {
    return db_;
}

std::shared_ptr<transaction_context> get_transaction_context(transaction_handle arg) {
    auto [db, tx] = cast(arg.db(), arg.get());
    (void) db;
    return tx;
}

}
