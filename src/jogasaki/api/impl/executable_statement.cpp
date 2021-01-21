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
#include "executable_statement.h"

namespace jogasaki::api::impl {

executable_statement::executable_statement(
    std::shared_ptr<plan::executable_statement> body,
    std::shared_ptr<memory::lifo_paged_memory_resource> resource
) :
    body_(std::move(body)),
    resource_(std::move(resource))
{}

std::shared_ptr<plan::executable_statement> const& executable_statement::body() const noexcept {
    return body_;
}

std::shared_ptr<memory::lifo_paged_memory_resource> const& executable_statement::resource() const noexcept {
    return resource_;
}

}

