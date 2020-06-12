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
#pragma once

#include <unordered_set>
#include <optional>

#include <takatori/util/optional_ptr.h>
#include <jogasaki/model/graph.h>
#include <jogasaki/request_context.h>
#include <jogasaki/executor/common/step.h>

namespace jogasaki::executor::common {

/**
 * @brief graph common implementation
 */
class graph : public model::graph {
public:
    graph() : context_(std::make_shared<request_context>()) {};

    explicit graph(std::shared_ptr<request_context> context) noexcept : context_(std::move(context)) {}

    [[nodiscard]] takatori::util::sequence_view<std::unique_ptr<model::step> const> steps() const noexcept override {
        return takatori::util::sequence_view(steps_);
    }

    takatori::util::optional_ptr<model::step> find_step(model::step::identity_type id) noexcept override {
        if (id < steps_.size()) {
            return takatori::util::optional_ptr<model::step>(steps_[id].get());
        }
        return takatori::util::optional_ptr<model::step>{};
    }

    // TODO graph should have request context?
    void context(std::shared_ptr<request_context> context) {
        context_ = std::move(context);
    }

    [[nodiscard]] std::shared_ptr<request_context> const& context() const noexcept override {
        return context_;
    }

    model::step& insert(std::unique_ptr<model::step> step) {
        auto impl = static_cast<common::step*>(step.get()); //NOLINT
        impl->owner(this);
        impl->id(steps_.size());
        auto& p = steps_.emplace_back(std::move(step));
        return *p;
    }

    template <class T, class ... Args>
    T& emplace(Args&& ... args) {
        auto n = steps_.size();
        auto& step = steps_.emplace_back(std::make_unique<T>(std::forward<Args>(args)...));
        auto* impl = static_cast<T*>(step.get()); //NOLINT
        impl->owner(this);
        impl->id(n);
        return *impl;
    }

    void reserve(std::size_t n) {
        steps_.reserve(n);
    }

    void clear() noexcept {
        steps_.clear();
    }

    [[nodiscard]] std::size_t size() const noexcept {
        return steps_.size();
    }

    static std::shared_ptr<graph> const& undefined() {
        static std::shared_ptr<graph> undefined = std::make_shared<graph>();
        return undefined;
    }
private:
    std::vector<std::unique_ptr<model::step>> steps_{};
    std::shared_ptr<request_context> context_{};
};

}
