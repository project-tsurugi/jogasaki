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
#include <memory>
#include <utility>

#include <jogasaki/api/commit_option.h>
#include <jogasaki/api/data_channel.h>
#include <jogasaki/api/error_info.h>
#include <jogasaki/api/executable_statement.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/impl/error_info.h>
#include <jogasaki/api/parameter_set.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/statement_handle.h>
#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/api/transaction_handle_internal.h>
#include <jogasaki/error/error_info.h>
#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/executor.h>
#include <jogasaki/request_info.h>
#include <jogasaki/request_statistics.h>
#include <jogasaki/status.h>
#include <jogasaki/transaction_context.h>

namespace jogasaki::api {

transaction_handle::transaction_handle(
    std::uintptr_t ptr,
    std::size_t surrogate_id
) noexcept:
    body_(ptr),
    surrogate_id_(surrogate_id)
{}

std::uintptr_t transaction_handle::get() const noexcept {
    return body_;
}

transaction_handle::operator std::size_t() const noexcept {
    return std::hash<transaction_handle>{}(*this);
}

transaction_handle::operator bool() const noexcept {
    return surrogate_id_ != 0;
}

transaction_context* tx(std::uintptr_t arg) {
    return reinterpret_cast<transaction_context*>(arg);  //NOLINT
}

std::shared_ptr<transaction_context> cast(std::uintptr_t tx, std::size_t surrogate_id) {
    return global::database_impl()->find_transaction(transaction_handle{tx, surrogate_id});
}

status transaction_handle::commit(api::commit_option option) {  //NOLINT(readability-make-member-function-const, performance-unnecessary-value-param)
    auto tx = cast(body_, surrogate_id_);
    if(! tx) return status::err_invalid_argument;
    return executor::commit(*global::database_impl(), tx, option);
}

void transaction_handle::commit_async(callback on_completion) {  //NOLINT(readability-make-member-function-const)
    commit_async([on_completion=std::move(on_completion)](
        status st,
        std::shared_ptr<api::error_info> info //NOLINT(performance-unnecessary-value-param)
    ) {
        on_completion(st, (info ? info->message() : ""));
    });
}

void transaction_handle::commit_async(  //NOLINT(readability-make-member-function-const,performance-unnecessary-value-param)
    error_info_callback on_completion,  //NOLINT(performance-unnecessary-value-param)
    commit_option opt,  //NOLINT(performance-unnecessary-value-param)
    request_info const& req_info
) {
    auto tx = cast(body_, surrogate_id_);
    if(! tx) {
        auto res = status::err_invalid_argument;
        on_completion(res,
            api::impl::error_info::create(
                create_error_info(error_code::transaction_not_found_exception, "invalid tx handle", res)
            )
        );
        return;
    }
    executor::commit_async(
        *global::database_impl(),
        tx,
        [on_completion](status st, std::shared_ptr<error::error_info> info) {
            on_completion(st, api::impl::error_info::create(std::move(info)));
        },
        opt,
        req_info
    );
}

status transaction_handle::abort(request_info const& req_info) {  //NOLINT(readability-make-member-function-const)
    return abort_transaction(req_info);
}

status transaction_handle::abort_transaction(request_info const& req_info) {  //NOLINT(readability-make-member-function-const)
    auto tx = cast(body_, surrogate_id_);
    if(! tx) return status::err_invalid_argument;
    executor::abort_transaction(tx, req_info);
    return status::ok;
}

status transaction_handle::execute(  //NOLINT(readability-make-member-function-const)
    executable_statement& statement,
    request_info const& req_info
) {
    auto tx = cast(body_, surrogate_id_);
    if(! tx) return status::err_invalid_argument;
    std::unique_ptr<api::result_set> result{};
    std::shared_ptr<error::error_info> info{};
    std::shared_ptr<request_statistics> stats{};
    return executor::execute(*global::database_impl(), tx, statement, result,
                             info, stats, req_info);
}

status transaction_handle::execute(  //NOLINT(readability-make-member-function-const)
    executable_statement& statement,
    std::unique_ptr<result_set>& result,
    request_info const& req_info
) {
    auto tx = cast(body_, surrogate_id_);
    if(! tx) return status::err_invalid_argument;
    std::shared_ptr<error::error_info> info{};
    std::shared_ptr<request_statistics> stats{};
    return executor::execute(*global::database_impl(), tx, statement, result,
                             info, stats, req_info);
}

status transaction_handle::execute( //NOLINT
    api::statement_handle prepared,
    std::shared_ptr<api::parameter_set> parameters,
    std::unique_ptr<result_set>& result,
    request_info const& req_info
) {
    auto tx = cast(body_, surrogate_id_);
    if(! tx) return status::err_invalid_argument;
    std::shared_ptr<error::error_info> info{};
    std::shared_ptr<request_statistics> stats{};
    return executor::execute(*global::database_impl(), tx, prepared,
                             std::move(parameters), result, info, stats,
                             req_info);
}

bool transaction_handle::execute_async(   //NOLINT(readability-make-member-function-const)
    maybe_shared_ptr<executable_statement> const& statement,
    transaction_handle::callback on_completion,
    request_info const& req_info
) {
    return execute_async(statement, [on_completion=std::move(on_completion)](
        status st,
        std::shared_ptr<api::error_info> info,  //NOLINT(performance-unnecessary-value-param)
        std::shared_ptr<request_statistics> stats  //NOLINT(performance-unnecessary-value-param)
    ){
        (void) stats;
        on_completion(st, (info ? info->message() : ""));
    }, req_info);
}

bool transaction_handle::execute_async(   //NOLINT(readability-make-member-function-const)
    maybe_shared_ptr<executable_statement> const& statement,
    transaction_handle::error_info_stats_callback on_completion,  //NOLINT(performance-unnecessary-value-param)
    request_info const& req_info
) {
    auto tx = cast(body_, surrogate_id_);
    if(! tx) {
        auto res = status::err_invalid_argument;
        on_completion(res,
            api::impl::error_info::create(
                create_error_info(error_code::transaction_not_found_exception, "invalid tx handle", res)
            ),
            nullptr
        );
        return true;
    }
    return executor::execute_async(
        *global::database_impl(),
        tx,
        statement,
        nullptr,
        [on_completion](
            status st,
            std::shared_ptr<error::error_info> info,
            std::shared_ptr<request_statistics> stats
        ) {
            on_completion(st, api::impl::error_info::create(std::move(info)), std::move(stats));
        },
        req_info
    );
}

bool transaction_handle::execute_async(  //NOLINT(readability-make-member-function-const)
    maybe_shared_ptr<executable_statement> const& statement,
    maybe_shared_ptr<data_channel> const& channel,
    transaction_handle::callback on_completion,
    request_info const& req_info
) {
    return execute_async(statement, channel, [on_completion=std::move(on_completion)](
        status st,
        std::shared_ptr<api::error_info> info,  //NOLINT(performance-unnecessary-value-param)
        std::shared_ptr<request_statistics> stats  //NOLINT(performance-unnecessary-value-param)
    ) {
        (void) stats;
        on_completion(st, (info ? info->message() : ""));
    }, req_info);
}

bool transaction_handle::execute_async(  //NOLINT(readability-make-member-function-const)
    maybe_shared_ptr<executable_statement> const& statement,
    maybe_shared_ptr<data_channel> const& channel,
    transaction_handle::error_info_stats_callback on_completion,  //NOLINT(performance-unnecessary-value-param)
    request_info const& req_info
) {
    auto tx = cast(body_, surrogate_id_);
    if(! tx) {
        auto res = status::err_invalid_argument;
        on_completion(res,
            api::impl::error_info::create(
                create_error_info(error_code::transaction_not_found_exception, "invalid tx handle", res)
            ),
            nullptr
        );
        return true;
    }
    return executor::execute_async(
        *global::database_impl(),
        tx,
        statement,
        channel,
        [on_completion](
            status st,
            std::shared_ptr<error::error_info> info,
            std::shared_ptr<request_statistics> stats
        ) {
            on_completion(st, api::impl::error_info::create(std::move(info)), std::move(stats));
        },
        req_info
    );
}

transaction_handle::transaction_handle(
    void* ptr,
    std::size_t surrogate_id
) noexcept:
    body_(reinterpret_cast<std::uintptr_t>(ptr)),  //NOLINT
    surrogate_id_(surrogate_id)
{}

bool transaction_handle::is_ready_unchecked() const {
    return tx(body_)->is_ready();
}

std::string_view transaction_handle::transaction_id_unchecked() const noexcept {
    return tx(body_)->transaction_id();
}

std::string_view transaction_handle::transaction_id() const noexcept {
    auto tx = cast(body_, surrogate_id_);
    if(! tx) {
        return {};
    }
    return tx->transaction_id();
}

status transaction_handle::error_info(std::shared_ptr<api::error_info>& out) const noexcept {
    out = {};
    auto tx = cast(body_, surrogate_id_);
    if(! tx) return status::err_invalid_argument;
    if(tx->error_info()) {
        out = api::impl::error_info::create(tx->error_info());
    }
    return status::ok;
}

std::size_t transaction_handle::surrogate_id() const noexcept {
    return surrogate_id_;
}

std::shared_ptr<transaction_context> get_transaction_context(transaction_handle arg) {
    return cast(arg.get(), arg.surrogate_id());
}

}
