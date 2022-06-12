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

#include <jogasaki/api/impl/transaction.h>

namespace jogasaki::api {

transaction_handle::transaction_handle(std::uintptr_t arg) noexcept:
    body_(arg)
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

status transaction_handle::commit() {  //NOLINT(readability-make-member-function-const)
    return reinterpret_cast<api::impl::transaction*>(body_)->commit();  //NOLINT
}

status transaction_handle::abort() {  //NOLINT(readability-make-member-function-const)
    return reinterpret_cast<api::impl::transaction*>(body_)->abort();  //NOLINT
}

status transaction_handle::execute(executable_statement& statement) {  //NOLINT(readability-make-member-function-const)
    return reinterpret_cast<api::impl::transaction*>(body_)->execute(statement);  //NOLINT
}

status transaction_handle::execute(executable_statement& statement, std::unique_ptr<result_set>& result) {  //NOLINT(readability-make-member-function-const)
    return reinterpret_cast<api::impl::transaction*>(body_)->execute(statement, result);  //NOLINT
}

status transaction_handle::execute(
    api::statement_handle prepared,
    std::shared_ptr<api::parameter_set> parameters,
    std::unique_ptr<result_set>& result) {
    return reinterpret_cast<api::impl::transaction*>(body_)->execute(prepared, std::move(parameters), result);  //NOLINT
}
bool transaction_handle::execute_async(maybe_shared_ptr<executable_statement> const& statement,  //NOLINT(readability-make-member-function-const)
    transaction_handle::callback on_completion) {
    return reinterpret_cast<api::impl::transaction*>(body_)->execute_async(  //NOLINT
        statement,
        std::move(on_completion)
    );
}

bool transaction_handle::execute_async(maybe_shared_ptr<executable_statement> const& statement,  //NOLINT(readability-make-member-function-const)
    maybe_shared_ptr<data_channel> const& channel, transaction_handle::callback on_completion) {
    return reinterpret_cast<api::impl::transaction*>(body_)->execute_async(  //NOLINT
        statement,
        channel,
        std::move(on_completion)
    );
}

transaction_handle::transaction_handle(void* arg) noexcept:
    body_(reinterpret_cast<std::uintptr_t>(arg))  //NOLINT
{}
}
