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
#include <takatori/util/downcast.h>
#include <takatori/util/sequence_view.h>

#include <jogasaki/model/graph.h>
#include <jogasaki/request_context.h>
#include <jogasaki/executor/common/step.h>

namespace jogasaki::executor::common {

using takatori::util::unsafe_downcast;
using takatori::util::sequence_view;
using takatori::util::optional_ptr;

/**
 * @brief graph common implementation
 */
class graph : public model::graph {
public:
    graph() = default;

    [[nodiscard]] sequence_view<std::unique_ptr<model::step> const> steps() const noexcept override;

    [[nodiscard]] optional_ptr<model::step> find_step(model::step::identity_type id) noexcept override;

    model::step& insert(std::unique_ptr<model::step> step);

    template <class T, class ... Args>
    T& emplace(Args&& ... args) {
        auto n = steps_.size();
        auto& step = steps_.emplace_back(std::make_unique<T>(std::forward<Args>(args)...));
        auto* impl = unsafe_downcast<T>(step.get()); //NOLINT
        impl->owner(this);
        impl->id(n);
        return *impl;
    }

    void reserve(std::size_t n);

    void clear() noexcept;

    [[nodiscard]] std::size_t size() const noexcept;

    [[nodiscard]] static std::shared_ptr<graph> const& undefined();

private:
    std::vector<std::unique_ptr<model::step>> steps_{};
};

}
