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

namespace dc::test {

using namespace executor;

class isolated_process_task : public common::task {
public:
    isolated_process_task() = default;
    ~isolated_process_task() override = default;
    isolated_process_task(isolated_process_task&& other) noexcept = default;
    isolated_process_task& operator=(isolated_process_task&& other) noexcept = default;
    isolated_process_task(channel* channel, model::step* src) : channel_(channel), src_(src) {}
    model::task_result operator()() override {
        LOG(INFO) << "isolated_process_task executed. count: " << count_;
        channel_->emplace(event_kind_tag<event_kind::task_completed>, src_->id(), id());
        ++count_;
        return count_ < limit_ ? model::task_result::proceed : model::task_result::complete;
    }
private:
    channel* channel_{};
    model::step* src_{};
    std::size_t count_{0};
    std::size_t limit_{3};
};

class isolated_process_flow : public common::flow {
public:
    isolated_process_flow() = default;
    ~isolated_process_flow() = default;
    isolated_process_flow(exchange::step* downstream, model::step* step, channel* ch) : downstream_(downstream), step_(step), channel_(ch) {}
    takatori::util::sequence_view<std::unique_ptr<model::task>> create_tasks() override {
        tasks_.emplace_back(std::make_unique<isolated_process_task>(channel_, step_));
        return tasks_;
    }

    takatori::util::sequence_view<std::unique_ptr<model::task>> create_pretask(port_index_type) override {
        return {};
    }
    [[nodiscard]] common::step_kind kind() const noexcept override {
        return common::step_kind::process;
    }
private:
    std::vector<std::unique_ptr<model::task>> tasks_{};
    exchange::step* downstream_{};
    model::step* step_{};
    channel* channel_{};
};

class isolated_process : public process::step {
public:
    isolated_process() : step(0, 0) {};
    ~isolated_process() = default;
    isolated_process(isolated_process&& other) noexcept = default;
    isolated_process& operator=(isolated_process&& other) noexcept = default;
    isolated_process(model::graph* owner) {
        graph_ = owner;
    }
    std::size_t max_partitions() const override {
        return step::max_partitions();
    }

    void activate() override {
        auto ch = graph_ ? &graph_->get_channel() : nullptr;
//        auto p = dynamic_cast<exchange::step*>(output_ports()[0]->opposites()[0]->owner());
        data_flow_object_ = std::make_unique<isolated_process_flow>(nullptr, this, ch);
    }
private:
    std::vector<std::unique_ptr<model::task>> tasks_{};
};

} // namespace
