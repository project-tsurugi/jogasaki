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

#include <vector>
#include <optional>

#include <takatori/util/optional_ptr.h>
#include <model/graph.h>
#include <executor/common/step.h>

namespace jogasaki::executor::common {

class graph : public model::graph {
public:
    graph() = default;
    ~graph() override = default;
    graph(graph&& other) noexcept = default;
    graph& operator=(graph&& other) noexcept = default;

    explicit graph(std::vector<std::unique_ptr<model::step>>&& steps) : steps_(std::move(steps)) {
        for(auto&& s: steps_) {
            static_cast<step*>(s.get())->set_owner(this); //NOLINT
        }
    }

    [[nodiscard]] takatori::util::sequence_view<std::unique_ptr<model::step> const> steps() const override {
        return takatori::util::sequence_view(steps_);
    }

    takatori::util::optional_ptr<model::step> find_step(model::step::identity_type id) override {
        for(auto&& x : steps_) {
            if (x->id() == id) {
                return takatori::util::optional_ptr<model::step>(*x);
            }
        }
        return takatori::util::optional_ptr<model::step>{};
    }

    channel& get_channel() override {
        return channel_;
    }

    void insert(std::unique_ptr<model::step>&& step) {
        auto impl = static_cast<common::step*>(step.get()); //NOLINT
        impl->set_owner(this);
        impl->set_id(steps_count_++);
        steps_.emplace_back(std::move(step));
    }

private:
    std::size_t steps_count_{};
    std::vector<std::unique_ptr<model::step>> steps_{};
    channel channel_{};
};

}
