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
#include "graph.h"

#include <unordered_set>
#include <optional>

#include <takatori/util/optional_ptr.h>
#include <takatori/util/downcast.h>

#include <jogasaki/model/graph.h>
#include <jogasaki/request_context.h>
#include <jogasaki/executor/common/step.h>

namespace jogasaki::executor::common {

using takatori::util::unsafe_downcast;

graph::graph(request_context& context) noexcept: context_(std::addressof(context)) {}

takatori::util::sequence_view<std::unique_ptr<model::step> const> graph::steps() const noexcept {
    return takatori::util::sequence_view(steps_);
}

optional_ptr<model::step> graph::find_step(model::step::identity_type id) noexcept {
    if (id < steps_.size()) {
        return takatori::util::optional_ptr<model::step>(steps_[id].get());
    }
    return takatori::util::optional_ptr<model::step>{};
}

void graph::context(request_context& context) {
    context_ = std::addressof(context);
}

request_context* graph::context() const noexcept {
    return context_;
}

model::step& graph::insert(std::unique_ptr<model::step> step) {
    auto impl = unsafe_downcast<common::step>(step.get()); //NOLINT
    impl->owner(this);
    impl->id(steps_.size());
    auto& p = steps_.emplace_back(std::move(step));
    return *p;
}

void graph::reserve(std::size_t n) {
    steps_.reserve(n);
}

void graph::clear() noexcept {
    steps_.clear();
}

std::size_t graph::size() const noexcept {
    return steps_.size();
}

std::shared_ptr<graph> const& graph::undefined() {
    static std::shared_ptr<graph> undefined = std::make_shared<graph>();
    return undefined;
}
}
