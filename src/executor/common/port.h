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

#include <model/port.h>
#include <model/step.h>

namespace jogasaki::executor::common {

class port : public model::port {
public:
    port() = default;
    port(port_direction direction, port_kind kind, model::step* owner = nullptr) : direction_(direction), kind_(kind), owner_(owner) {}

    [[nodiscard]] takatori::util::sequence_view<model::port* const> opposites() const override {
        return opposites_;
    }
    void set_opposites(std::vector<model::port*>&& arg) {
        opposites_ = std::move(arg);
    }
    void set_owner(model::step* arg) override {
        owner_ = arg;
    }
    [[nodiscard]] port_kind kind() const override {
        return kind_;
    }
    [[nodiscard]] port_direction direction() const override {
        return direction_;
    }
    [[nodiscard]] model::step* const& owner() const override {
        return owner_;
    }
    void add_opposite(port* target) {
        opposites_.emplace_back(target);
        target->opposites_.emplace_back(this);
    }

private:
    port_direction direction_{};
    port_kind kind_{};
    std::vector<model::port*> opposites_{};
    model::step* owner_{};
};

}

