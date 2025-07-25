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
#pragma once

#include <vector>

#include <takatori/util/sequence_view.h>

#include <jogasaki/model/port.h>
#include <jogasaki/model/step.h>

namespace jogasaki::executor::common {

using takatori::util::sequence_view;

class port : public model::port {
public:
    port() = default;

    port(port_direction direction, port_kind kind, model::step* owner = nullptr) noexcept;

    [[nodiscard]] sequence_view<model::port* const> opposites() const override;
    void set_opposites(std::vector<model::port*>&& arg);
    void owner(model::step* arg) override;
    [[nodiscard]] port_kind kind() const override;
    [[nodiscard]] port_direction direction() const override;
    [[nodiscard]] model::step* const& owner() const override;
    void add_opposite(port* target);

private:
    port_direction direction_{};
    port_kind kind_{};
    std::vector<model::port*> opposites_{};
    model::step* owner_{};
};

}

