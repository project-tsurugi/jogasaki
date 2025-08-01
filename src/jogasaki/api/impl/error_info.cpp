/*
 * Copyright 2018-2024 Project Tsurugi.
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
#include "error_info.h"

#include <utility>

#include <jogasaki/error/error_info.h>

namespace jogasaki::api::impl {

error_info::error_info(std::shared_ptr<error::error_info> body) noexcept:
    body_(std::move(body))
{}

std::string_view error_info::message() const noexcept {
    return body_->message();
}

jogasaki::error_code error_info::code() const noexcept {
    return body_->code();
}

jogasaki::status error_info::status() const noexcept {
    return body_->status();
}

std::string_view error_info::supplemental_text() const noexcept {
    return body_->supplemental_text();
}

void error_info::write_to(std::ostream &os) const noexcept {
    os << *body_;
}

std::shared_ptr<api::impl::error_info> error_info::create(std::shared_ptr<error::error_info> body) noexcept {
    if(! body) {
        return {};
    }
    return std::shared_ptr<api::impl::error_info>(new api::impl::error_info(std::move(body)));  //NOLINT
}

std::shared_ptr<error::error_info> const &error_info::body() const noexcept {
    return body_;
}

}

