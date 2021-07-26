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

#include <jogasaki/model/task.h>
#include <jogasaki/model/task.h>
#include <tateyama/task_scheduler.h>
#include <tateyama/context.h>
#include "task_scheduler.h"
#include "thread_params.h"

namespace jogasaki::scheduler {

class flat_task {
public:
    flat_task() = default;
    ~flat_task() = default;
    flat_task(flat_task const& other) = default;
    flat_task& operator=(flat_task const& other) = default;
    flat_task(flat_task&& other) noexcept = default;
    flat_task& operator=(flat_task&& other) noexcept = default;

    explicit flat_task(std::shared_ptr<model::task> origin) noexcept :
        origin_(std::move(origin))
    {}

    [[nodiscard]] std::shared_ptr<model::task> const& origin() const noexcept {
        return origin_;
    }

    void operator()(tateyama::context& ctx) {
        (void)ctx;
        auto res = (*origin_)();
        (void)res;
    }

private:
    std::shared_ptr<model::task> origin_{};
};
/**
 * @brief task scheduler using multiple threads
 */
class cache_align stealing_task_scheduler : public task_scheduler {
public:

    stealing_task_scheduler() = default;
    ~stealing_task_scheduler() override = default;
    stealing_task_scheduler(stealing_task_scheduler const& other) = delete;
    stealing_task_scheduler& operator=(stealing_task_scheduler const& other) = delete;
    stealing_task_scheduler(stealing_task_scheduler&& other) noexcept = delete;
    stealing_task_scheduler& operator=(stealing_task_scheduler&& other) noexcept = delete;

    explicit stealing_task_scheduler(thread_params params) :
        scheduler_cfg_(create_scheduler_cfg(params)),
        scheduler_(scheduler_cfg_)
    {}

    /**
     * @brief schedule the task
     * @param task the task to schedule
     * @pre scheduler is started
     */
    void schedule_task(std::shared_ptr<model::task> const& task) override {
        scheduler_.schedule(flat_task{task});
    }

    /**
     * @brief wait for the scheduler to proceed
     * @details this is no-op for multi-thread scheduler
     */
    void wait_for_progress() override {
        // do nothing
    }

    /**
     * @brief start the scheduler so that it's ready to accept request
     */
    void start() override {
        scheduler_.start();
    }

    /**
     * @brief stop the scheduler joining all the running tasks and
     * canceling ones that are submitted but not yet executed
     */
    void stop() override {
        scheduler_.stop();
    }

    /**
     * @return kind of the task scheduler
     */
    [[nodiscard]] task_scheduler_kind kind() const noexcept override {
        return task_scheduler_kind::stealing;
    }

private:
    tateyama::task_scheduler_cfg scheduler_cfg_{};
    tateyama::task_scheduler<flat_task> scheduler_;

    tateyama::task_scheduler_cfg create_scheduler_cfg(thread_params params) {
        tateyama::task_scheduler_cfg ret{};
        ret.thread_count(params.threads());
        ret.force_numa_node(params.force_numa_node());
        ret.core_affinity(params.is_set_core_affinity());
        ret.assign_numa_nodes_uniformly(params.assign_numa_nodes_uniformly());
        ret.initial_core(params.inititial_core());
        ret.stealing_enabled(params.stealing_enabled());
        return ret;
    }
};

}



