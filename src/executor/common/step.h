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

#include <cassert>
#include <model/step.h>
#include <model/graph.h>
#include <executor/common/port.h>
#include <executor/common/step_kind.h>
#include <executor/common/flow.h>

namespace jogasaki::executor::common {

class step : public model::step {
public:
    using size_type = std::size_t;

    using port_index_type = std::size_t;

    explicit step(size_type inputs = 1, size_type outputs = 1, size_type subinputs = 0) {
        main_input_ports_.reserve(inputs);
        sub_input_ports_.reserve(subinputs);
        output_ports_.reserve(outputs);
        for(std::size_t i=0; i < inputs; ++i) {
            main_input_ports_.emplace_back(std::make_unique<port>(port_direction::input, port_kind::main, this));
        }
        for(std::size_t i=0; i < subinputs; ++i) {
            sub_input_ports_.emplace_back(std::make_unique<port>(port_direction::input, port_kind::sub, this));
        }
        for(std::size_t i=0; i < outputs; ++i) {
            output_ports_.emplace_back(std::make_unique<port>(port_direction::output, port_kind::main, this));
        }
    }

    [[nodiscard]] identity_type id() const override {
        return id_;
    }
    void set_main_input_ports(std::vector<std::unique_ptr<model::port>>&& arg) {
        main_input_ports_ = std::move(arg);
        for(auto&& p: main_input_ports_) {
            p->set_owner(this);
        }
    }
    void set_sub_input_ports(std::vector<std::unique_ptr<model::port>>&& arg) {
        sub_input_ports_ = std::move(arg);
        for(auto&& p: sub_input_ports_) {
            p->set_owner(this);
        }
    }
    void set_output_ports(std::vector<std::unique_ptr<model::port>>&& arg) {
        output_ports_ = std::move(arg);
        for(auto&& p: output_ports_) {
            p->set_owner(this);
        }
    }
    [[nodiscard]] takatori::util::sequence_view<std::unique_ptr<model::port> const> input_ports() const override {
        return main_input_ports_;
    }
    [[nodiscard]] takatori::util::sequence_view<std::unique_ptr<model::port> const> subinput_ports() const override {
        return sub_input_ports_;
    }
    [[nodiscard]] takatori::util::sequence_view<std::unique_ptr<model::port> const> output_ports() const override {
        return output_ports_;
    }
    [[nodiscard]] model::graph* owner() const override {
        return graph_;
    }
    void set_owner(model::graph* g) noexcept {
        graph_ = g;
    }
    void set_id(identity_type id) noexcept {
        id_ = id;
    }
    [[nodiscard]] virtual step_kind kind() const noexcept = 0;

    void deactivate() override {

    }
    void notify_prepared() override {

    }
    bool has_subinput() override {
        return !sub_input_ports_.empty();
    }

    port_index_type sub_input_port_index(step const* source) {
        for(port_index_type i=0; i < sub_input_ports_.size(); ++i) {
            auto* s = sub_input_ports_[i]->opposites()[0]->owner();
            if (source->id() == s->id()) {
                return i;
            }
        }
        return port_index_type(-1);
    }

    void connect_to(step& downstream, port_index_type src = 0, port_index_type target = 0) {
        dynamic_cast<port*>(output_ports_[src].get())->add_opposite(dynamic_cast<port*>(downstream.main_input_ports_[target].get()));
    }

    void connect_to_sub(step& downstream, port_index_type src = 0, port_index_type target = 0) {
        dynamic_cast<port*>(output_ports_[src].get())->add_opposite(dynamic_cast<port*>(downstream.sub_input_ports_[target].get()));
    }

    friend step& operator<<(step& downstream, step& upstream) {
        upstream.connect_to(downstream);
        return downstream;
    }
    friend step& operator>>(step& upstream, step& downstream) {
        upstream.connect_to(downstream);
        return upstream;
    }

    takatori::util::sequence_view<std::unique_ptr<model::task>> create_tasks() override {
        assert(data_flow_object_ != nullptr); //NOLINT
        return data_flow_object_->create_tasks();
    }

    takatori::util::sequence_view<std::unique_ptr<model::task>> create_pretask(port_index_type subinput) override {
        assert(data_flow_object_ != nullptr); //NOLINT
        return data_flow_object_->create_pretask(subinput);
    }
    flow& data_flow_object() const noexcept {
        return *data_flow_object_;
    }
protected:
    identity_type id_{};
    std::vector<std::unique_ptr<model::port>> main_input_ports_{};
    std::vector<std::unique_ptr<model::port>> sub_input_ports_{};
    std::vector<std::unique_ptr<model::port>> output_ports_{};
    model::graph* graph_{};
    std::unique_ptr<flow> data_flow_object_{};

    std::ostream& write_to(std::ostream& out) const override {
        using namespace std::string_view_literals;
        return out << to_string_view(kind()) << "[id="sv << std::to_string(static_cast<identity_type>(id_)) << "]"sv;
    };
    // data flow context below

};

}
