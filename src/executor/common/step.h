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

#include <model/step.h>
#include <model/graph.h>
#include <executor/common/port.h>
#include <executor/common/step_kind.h>
#include <executor/common/flow.h>

namespace jogasaki::executor::common {

/**
 * @brief step common implementation
 * @details represents connectivity among steps
 */
class step : public model::step {
public:
    using number_of_ports = std::size_t;

    using port_index = std::size_t;

    explicit step(number_of_ports inputs = 1, number_of_ports outputs = 1, number_of_ports subinputs = 0);

    [[nodiscard]] identity_type id() const override;

//    void set_main_input_ports(std::vector<std::unique_ptr<model::port>>&& arg);
//    void set_sub_input_ports(std::vector<std::unique_ptr<model::port>>&& arg);
//    void set_output_ports(std::vector<std::unique_ptr<model::port>>&& arg);

    [[nodiscard]] takatori::util::sequence_view<std::unique_ptr<model::port> const> input_ports() const override;
    [[nodiscard]] takatori::util::sequence_view<std::unique_ptr<model::port> const> subinput_ports() const override;
    [[nodiscard]] takatori::util::sequence_view<std::unique_ptr<model::port> const> output_ports() const override;

    /**
     * @brief accessor to owner graph of this step
     * @return owner graph
     */
    [[nodiscard]] model::graph* owner() const override;

    /**
     * @brief setter of owner graph of this step
     */
    void owner(model::graph* g) noexcept;

    /**
     * @brief setter of owner graph of this step
     */
    void id(identity_type id) noexcept;

    [[nodiscard]] virtual step_kind kind() const noexcept = 0;

    void deactivate() override;
    void notify_prepared() override;
    bool has_subinput() override;

    port_index sub_input_port_index(step const* source);

    void connect_to(step& downstream, port_index src = 0, port_index target = 0);

    void connect_to_sub(step& downstream, port_index src = 0, port_index target = 0);

    takatori::util::sequence_view<std::unique_ptr<model::task>> create_tasks() override;

    takatori::util::sequence_view<std::unique_ptr<model::task>> create_pretask(port_index subinput) override;

    [[nodiscard]] flow& data_flow_object() const noexcept;

protected:
    void data_flow_object(std::unique_ptr<flow> p) noexcept;
    [[nodiscard]] std::shared_ptr<class request_context> const& context() const noexcept;

private:
    identity_type id_{};
    std::vector<std::unique_ptr<model::port>> main_input_ports_{};
    std::vector<std::unique_ptr<model::port>> sub_input_ports_{};
    std::vector<std::unique_ptr<model::port>> output_ports_{};
    model::graph* owner_{};
    std::unique_ptr<flow> data_flow_object_{};

    std::ostream& write_to(std::ostream& out) const override;
};

step& operator<<(step& downstream, step& upstream);
step& operator>>(step& upstream, step& downstream);

}
