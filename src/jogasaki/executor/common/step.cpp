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
#include "step.h"

#include <cassert>

#include <jogasaki/executor/common/graph.h>

namespace jogasaki::executor::common {

step::step(
    number_of_ports inputs,
    number_of_ports outputs,
    number_of_ports subinputs
) :
    owner_(graph::undefined().get())
{
    main_input_ports_.reserve(inputs);
    sub_input_ports_.reserve(subinputs);
    output_ports_.reserve(outputs);
    for(number_of_ports i=0; i < inputs; ++i) {
        main_input_ports_.emplace_back(std::make_unique<port>(
            port_direction::input,
            port_kind::main, this)
        );
    }
    for(number_of_ports i=0; i < subinputs; ++i) {
        sub_input_ports_.emplace_back(std::make_unique<port>(
            port_direction::input,
            port_kind::sub, this)
        );
    }
    for(number_of_ports i=0; i < outputs; ++i) {
        output_ports_.emplace_back(std::make_unique<port>(
            port_direction::output,
            port_kind::main,
            this)
        );
    }
}

step::identity_type step::id() const {
    return id_;
}

sequence_view<std::unique_ptr<model::port> const> step::input_ports() const {
    return main_input_ports_;
}

sequence_view<std::unique_ptr<model::port> const> step::subinput_ports() const {
    return sub_input_ports_;
}

sequence_view<std::unique_ptr<model::port> const> step::output_ports() const {
    return output_ports_;
}

model::graph* step::owner() const {
    return owner_;
}

void step::owner(model::graph* g) noexcept {
    owner_ = g;
}

void step::id(identity_type id) noexcept {
    id_ = id;
}

void step::deactivate() {
    data_flow_object_.reset();
}

void step::notify_prepared() {

}

bool step::has_subinput() {
    return !sub_input_ports_.empty();
}

step::port_index step::sub_input_port_index(step const* source) {
    for(step::port_index i=0; i < sub_input_ports_.size(); ++i) {
        auto* s = sub_input_ports_[i]->opposites()[0]->owner();
        if (source->id() == s->id()) {
            return i;
        }
    }
    return port_index(-1);
}

void step::connect_to(step& downstream, port_index src, port_index target) {
    if (src == npos) {
        output_ports_.emplace_back(std::make_unique<port>(
            port_direction::output,
            port_kind::main,
            this)
            );
        src = output_ports_.size() - 1;
    }
    if (target == npos) {
        downstream.main_input_ports_.emplace_back(
            std::make_unique<port>(port_direction::input, port_kind::main, &downstream)
        );
        target = downstream.main_input_ports_.size() - 1;
    }
    dynamic_cast<port*>(output_ports_[src].get())->add_opposite(
        dynamic_cast<port*>(downstream.main_input_ports_[target].get())
    );
}

void step::connect_to_sub(step& downstream, port_index src, port_index target) {
    if (src == npos) {
        output_ports_.emplace_back(
            std::make_unique<port>(port_direction::output, port_kind::main, this)
        );
        src = output_ports_.size() - 1;
    }
    if (target == npos) {
        downstream.sub_input_ports_.emplace_back(
            std::make_unique<port>(port_direction::input, port_kind::sub, &downstream)
        );
        target = downstream.sub_input_ports_.size() - 1;
    }
    dynamic_cast<port*>(output_ports_[src].get())->add_opposite(
        dynamic_cast<port*>(downstream.sub_input_ports_[target].get())
    );
}

sequence_view<std::shared_ptr<model::task>> step::create_tasks() {
    assert(data_flow_object_ != nullptr); //NOLINT
    if (will_create_tasks_) {
        (*will_create_tasks_)(nullptr);
    }
    auto ret = data_flow_object_->create_tasks();
    if (did_create_tasks_) {
        (*did_create_tasks_)(nullptr);
    }
    return ret;
}

sequence_view<std::shared_ptr<model::task>> step::create_pretask(port_index subinput) {
    assert(data_flow_object_ != nullptr); //NOLINT
    return data_flow_object_->create_pretask(subinput);
}

flow& step::data_flow_object() const noexcept {
    return *data_flow_object_;
}

void step::data_flow_object(std::unique_ptr<flow> p) noexcept {
    data_flow_object_ = std::move(p);
}

class request_context* step::context() const noexcept {
    assert(owner_); //NOLINT
    return owner_->context();
}

std::ostream& step::write_to(std::ostream& out) const {
    using namespace std::string_view_literals;
    return out <<
        to_string_view(kind()) <<
        "[id="sv <<
        std::to_string(static_cast<identity_type>(id_)) <<
        "]"sv;
}

void step::will_create_tasks(std::shared_ptr<callback_type> arg) {
    will_create_tasks_ = std::move(arg);
}

void step::did_create_tasks(std::shared_ptr<callback_type> arg) {
    did_create_tasks_ = std::move(arg);
}

void step::did_start_task(std::shared_ptr<callback_type> arg) {
    did_start_task_ = std::move(arg);
}

std::shared_ptr<callback_type> const& step::did_start_task() {
    return did_start_task_;
}

void step::will_end_task(std::shared_ptr<callback_type> arg) {
    will_end_task_ = std::move(arg);
}

std::shared_ptr<callback_type> const& step::will_end_task() {
    return will_end_task_;
}

step& operator<<(step& downstream, step& upstream) {
    upstream.connect_to(downstream);
    return upstream;
}

step& operator>>(step& upstream, step& downstream) {
    upstream.connect_to(downstream);
    return downstream;
}

}
