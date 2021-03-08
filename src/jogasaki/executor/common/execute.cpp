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
#include "execute.h"

namespace jogasaki::executor::common {

execute::execute(std::shared_ptr<common::graph> graph) noexcept:
    operators_(std::move(graph)),
    context_(operators_->context())
{}

void execute::context(request_context& context) {
    context_ = std::addressof(context);
}

request_context* execute::context() const noexcept {
    return context_;
}

model::statement_kind execute::kind() const noexcept {
    return model::statement_kind::execute;
}

common::graph& execute::operators() const noexcept {
    return *operators_;
}

}
