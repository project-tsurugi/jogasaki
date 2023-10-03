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
#include <jogasaki/api/statement_handle.h>

#include <cstdint>
#include <type_traits>
#include <ostream>
#include <functional>

#include <jogasaki/api/impl/prepared_statement.h>

namespace jogasaki::api {

api::record_meta const* statement_handle::meta() const noexcept {
    return reinterpret_cast<impl::prepared_statement*>(body_)->meta();  //NOLINT
}

statement_handle::statement_handle(void* arg) noexcept:
    body_(reinterpret_cast<std::uintptr_t>(arg))  //NOLINT
{}

statement_handle::statement_handle(std::uintptr_t arg) noexcept:
    body_(arg)
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

}
