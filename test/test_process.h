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

namespace jogasaki::test {

using namespace executor;

class test_process_task : public common::task {
public:
    test_process_task() = default;
    ~test_process_task() override = default;
    test_process_task(test_process_task&& other) noexcept = default;
    test_process_task& operator=(test_process_task&& other) noexcept = default;
    test_process_task(channel* channel, model::step* src) : channel_(channel), src_(src) {}
    model::task_result operator()() override {
        LOG(INFO) << "test_process_task executed. count: " << count_;
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

class test_process_flow : public common::flow {
public:
    test_process_flow() = default;
    ~test_process_flow() = default;
    test_process_flow(exchange::step* downstream, model::step* step, channel* ch) : downstream_(downstream), step_(step), channel_(ch) {}
    takatori::util::sequence_view<std::unique_ptr<model::task>> create_tasks() override {
        tasks_.emplace_back(std::make_unique<test_process_task>(channel_, step_));
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

class test_process : public process::step {
public:
    test_process() : step(0, 0) {};
    ~test_process() = default;
    test_process(test_process&& other) noexcept = default;
    test_process& operator=(test_process&& other) noexcept = default;

    void activate() override {
        data_flow_object(std::make_unique<test_process_flow>(nullptr, this, channel()));
    }
    void deactivate() override {

    }
private:
};

} // namespace
