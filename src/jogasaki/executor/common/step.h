/*
 * Copyright 2018-2023 Project Tsurugi.
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

#include <cstddef>
#include <iosfwd>
#include <memory>
#include <vector>

#include <takatori/util/sequence_view.h>

#include <jogasaki/callback.h>
#include <jogasaki/executor/common/port.h>
#include <jogasaki/model/flow.h>
#include <jogasaki/model/graph.h>
#include <jogasaki/model/port.h>
#include <jogasaki/model/step.h>
#include <jogasaki/model/step_kind.h>
#include <jogasaki/model/task.h>

namespace jogasaki::executor::common {

using ::takatori::util::sequence_view;

/**
 * @brief step common implementation
 * @details represents connectivity among steps
 */
class step : public model::step {
public:
    using port_index = std::size_t;

    static constexpr port_index npos = static_cast<port_index>(-1);

    /**
     * @brief create empty object
     */
    step();

    [[nodiscard]] identity_type id() const override;
    [[nodiscard]] sequence_view<std::unique_ptr<model::port> const> input_ports() const override;
    [[nodiscard]] sequence_view<std::unique_ptr<model::port> const> subinput_ports() const override;
    [[nodiscard]] sequence_view<std::unique_ptr<model::port> const> output_ports() const override;

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

    [[nodiscard]] virtual model::step_kind kind() const noexcept = 0;

    void deactivate(request_context& rctx) override;
    void notify_prepared(request_context&) override;
    [[nodiscard]] bool has_subinput() override;

    [[nodiscard]] port_index sub_input_port_index(step const* source);

    void connect_to(step& downstream, port_index src = npos, port_index target = npos);

    void connect_to_sub(step& downstream, port_index src = npos, port_index target = npos);

    [[nodiscard]] sequence_view<std::shared_ptr<model::task>> create_tasks(request_context& rctx) override;

    [[nodiscard]] sequence_view<std::shared_ptr<model::task>> create_pretask(request_context& rctx, port_index subinput) override;

    [[nodiscard]] model::flow& data_flow_object(request_context& rctx) const noexcept;

    std::ostream& write_to(std::ostream& out) const override;

    /**
     * @brief sets callback before creating tasks
     * @param arg the callback
     */
    void will_create_tasks(std::shared_ptr<callback_type> arg);

    /**
     * @brief sets callback after creating tasks
     * @param arg the callback
     */
    void did_create_tasks(std::shared_ptr<callback_type> arg);

    /**
     * @brief sets callback just after starting task
     * @param arg the callback
     */
    void did_start_task(std::shared_ptr<callback_type> arg);

    /**
     * @brief accessor to callback
     */
    std::shared_ptr<callback_type> const& did_start_task();

    /**
     * @brief sets callback just before ending task
     * @param arg the callback
     */
    void will_end_task(std::shared_ptr<callback_type> arg);

    /**
     * @brief accessor to callback
     */
    std::shared_ptr<callback_type> const& will_end_task();

protected:

    void data_flow_object(request_context& rctx, std::unique_ptr<model::flow> p) const noexcept;

private:
    identity_type id_{};
    std::vector<std::unique_ptr<model::port>> main_input_ports_{};
    std::vector<std::unique_ptr<model::port>> sub_input_ports_{};
    std::vector<std::unique_ptr<model::port>> output_ports_{};
    model::graph* owner_{};
    std::shared_ptr<callback_type> will_create_tasks_{};
    std::shared_ptr<callback_type> did_create_tasks_{};
    std::shared_ptr<callback_type> did_start_task_{};
    std::shared_ptr<callback_type> will_end_task_{};
};

step& operator<<(step& downstream, step& upstream);
step& operator>>(step& upstream, step& downstream);

}
