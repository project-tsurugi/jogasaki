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
#include <jogasaki/configuration.h>

namespace jogasaki::scheduler {

class thread_params {
public:
    static constexpr std::size_t numa_node_unspecified = configuration::numa_node_unspecified;

    thread_params() = default;

    explicit thread_params(
        std::size_t threads,
        bool set_core_affinity = true,
        std::size_t initial_core = 1,
        bool assign_numa_nodes_uniformly = false,
        std::size_t randomize_memory_usage = 0,
        std::size_t force_numa_node = numa_node_unspecified,
        bool stealing_enabled = false,
        bool lazy_worker = false,
        bool use_preferred_worker_for_current_thread = false,
        std::size_t stealing_wait = 1,
        std::size_t task_polling_wait = 0,
        bool busy_worker = true,
        bool enable_watcher = false,
        std::size_t watcher_interval = 1000,
        std::size_t worker_try_count = 100000,
        std::size_t worker_suspend_timeout = 1000000
    ) :
        threads_(threads),
        set_core_affinity_(set_core_affinity),
        initial_core_(initial_core),
        assign_numa_nodes_uniformly_(assign_numa_nodes_uniformly),
        randomize_memory_usage_(randomize_memory_usage),
        force_numa_node_(force_numa_node),
        stealing_enabled_(stealing_enabled),
        lazy_worker_(lazy_worker),
        use_preferred_worker_for_current_thread_(use_preferred_worker_for_current_thread),
        stealing_wait_(stealing_wait),
        task_polling_wait_(task_polling_wait),
        busy_worker_(busy_worker),
        enable_watcher_(enable_watcher),
        watcher_interval_(watcher_interval),
        worker_try_count_(worker_try_count),
        worker_suspend_timeout_(worker_suspend_timeout)
    {}

    explicit thread_params(std::shared_ptr<configuration> const& cfg) :
        thread_params(
            cfg->thread_pool_size(),
            cfg->core_affinity(),
            cfg->initial_core(),
            cfg->assign_numa_nodes_uniformly(),
            cfg->randomize_memory_usage(),
            cfg->force_numa_node(),
            cfg->stealing_enabled(),
            cfg->lazy_worker(),
            cfg->use_preferred_worker_for_current_thread(),
            cfg->stealing_wait(),
            cfg->task_polling_wait(),
            cfg->busy_worker(),
            cfg->enable_watcher(),
            cfg->watcher_interval(),
            cfg->worker_try_count(),
            cfg->worker_suspend_timeout()
        )
    {}

    [[nodiscard]] std::size_t threads() const noexcept {
        return threads_;
    }

    [[nodiscard]] bool is_set_core_affinity() const noexcept {
        return set_core_affinity_;
    }

    [[nodiscard]] std::size_t inititial_core() const noexcept {
        return initial_core_;
    }

    [[nodiscard]] bool assign_numa_nodes_uniformly() const noexcept {
        return assign_numa_nodes_uniformly_;
    }

    [[nodiscard]] std::size_t randomize_memory_usage() const noexcept {
        return randomize_memory_usage_;
    }

    [[nodiscard]] std::size_t force_numa_node() const noexcept {
        return force_numa_node_;
    }

    [[nodiscard]] bool stealing_enabled() const noexcept {
        return stealing_enabled_;
    }

    [[nodiscard]] bool lazy_worker() const noexcept {
        return lazy_worker_;
    }

    [[nodiscard]] bool use_preferred_worker_for_current_thread() const noexcept {
        return use_preferred_worker_for_current_thread_;
    }

    [[nodiscard]] std::size_t stealing_wait() const noexcept {
        return stealing_wait_;
    }

    [[nodiscard]] std::size_t task_polling_wait() const noexcept {
        return task_polling_wait_;
    }

    [[nodiscard]] bool busy_worker() const noexcept {
        return busy_worker_;
    }

    void busy_worker(bool arg) noexcept {
        busy_worker_ = arg;
    }

    void enable_watcher(bool arg) noexcept {
        enable_watcher_ = arg;
    }

    [[nodiscard]] bool enable_watcher() const noexcept {
        return enable_watcher_;
    }

    [[nodiscard]] std::size_t watcher_interval() const noexcept {
        return watcher_interval_;
    }

    void watcher_interval(std::size_t arg) noexcept {
        watcher_interval_ = arg;
    }

    [[nodiscard]] std::size_t worker_try_count() const noexcept {
        return worker_try_count_;
    }

    void worker_try_count(std::size_t arg) noexcept {
        worker_try_count_ = arg;
    }

    [[nodiscard]] std::size_t worker_suspend_timeout() const noexcept {
        return worker_suspend_timeout_;
    }

    void worker_suspend_timeout(std::size_t arg) noexcept {
        worker_suspend_timeout_ = arg;
    }

private:
    std::size_t threads_{};
    bool set_core_affinity_{};
    std::size_t initial_core_{};
    bool assign_numa_nodes_uniformly_{};
    std::size_t randomize_memory_usage_{};
    std::size_t force_numa_node_{};
    bool stealing_enabled_{};
    bool lazy_worker_{};
    bool use_preferred_worker_for_current_thread_{};
    std::size_t stealing_wait_{};
    std::size_t task_polling_wait_{};
    bool busy_worker_{};
    bool enable_watcher_{};
    std::size_t watcher_interval_{};
    std::size_t worker_try_count_{};
    std::size_t worker_suspend_timeout_{};
};

} // namespace
