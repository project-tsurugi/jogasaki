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
#include "prepared_statement.h"

namespace jogasaki::api::impl {

std::shared_ptr<plan::prepared_statement> const& prepared_statement::body() const noexcept {
    return body_;
}

prepared_statement::prepared_statement(
    std::shared_ptr<plan::prepared_statement> body
) :
    body_(std::move(body)),
    meta_(body_->mirrors()->external_writer_meta() ?
        std::make_unique<impl::record_meta>(body_->mirrors()->external_writer_meta()) :
        nullptr
    )
{}

}
