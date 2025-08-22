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
#include <cstdint>

#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/impl/prepared_statement.h>
#include <jogasaki/api/record_meta.h>
#include <jogasaki/api/statement_handle.h>

namespace jogasaki::api {

statement_handle::statement_handle(
    void* arg,
    std::optional<std::size_t> session_id
) noexcept :
    body_(reinterpret_cast<std::uintptr_t>(arg)),  //NOLINT
    session_id_(session_id)  //NOLINT
{}

std::uintptr_t statement_handle::get() const noexcept {
    return body_;
}

std::optional<std::size_t> statement_handle::session_id() const noexcept {
    return session_id_;
}

statement_handle::operator std::size_t() const noexcept {
    return reinterpret_cast<std::size_t>(body_);  //NOLINT
}

statement_handle::operator bool() const noexcept {
    return body_ != 0;
}

std::shared_ptr<impl::prepared_statement> get_statement(statement_handle arg) {  //NOLINT(misc-use-internal-linkage) false positive
    return global::database_impl()->find_statement(arg);
}

bool statement_handle::has_result_records() const noexcept {
    auto stmt = get_statement(*this);
    if (! stmt) {
        return false;
    }
    return stmt->has_result_records();  //NOLINT
}

api::record_meta const* statement_handle::meta() const noexcept {
    auto stmt = get_statement(*this);
    if (! stmt) {
        return nullptr;
    }
    return stmt->meta();  //NOLINT
}

}  // namespace jogasaki::api
