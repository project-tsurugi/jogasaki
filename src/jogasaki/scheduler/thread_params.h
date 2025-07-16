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
#include <jogasaki/configuration.h>

namespace jogasaki::scheduler {

class thread_params {
public:
    static constexpr std::size_t numa_node_unspecified = configuration::numa_node_unspecified;

    thread_params() = default;

    explicit thread_params(
        std::size_t threads,
        bool set_core_affinity,
        std::size_t initial_core,
        bool assign_numa_nodes_uniformly,
        std::size_t randomize_memory_usage,
        std::size_t force_numa_node,
        bool stealing_enabled,
        bool use_preferred_worker_for_current_thread,
        std::size_t stealing_wait,
        std::size_t task_polling_wait,
        bool busy_worker,
        std::size_t watcher_interval,
        std::size_t worker_try_count,
        std::size_t worker_suspend_timeout,
        std::size_t thousandths_ratio_check_local_first
    ) :
        threads_(threads),
        set_core_affinity_(set_core_affinity),
        initial_core_(initial_core),
        assign_numa_nodes_uniformly_(assign_numa_nodes_uniformly),
        randomize_memory_usage_(randomize_memory_usage),
        force_numa_node_(force_numa_node),
        stealing_enabled_(stealing_enabled),
        use_preferred_worker_for_current_thread_(use_preferred_worker_for_current_thread),
        stealing_wait_(stealing_wait),
        task_polling_wait_(task_polling_wait),
        busy_worker_(busy_worker),
        watcher_interval_(watcher_interval),
        worker_try_count_(worker_try_count),
        worker_suspend_timeout_(worker_suspend_timeout),
        thousandths_ratio_check_local_first_(thousandths_ratio_check_local_first)
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
            cfg->use_preferred_worker_for_current_thread(),
            cfg->stealing_wait(),
            cfg->task_polling_wait(),
            cfg->busy_worker(),
            cfg->watcher_interval(),
            cfg->worker_try_count(),
            cfg->worker_suspend_timeout(),
            cfg->thousandths_ratio_check_local_first()
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

    [[nodiscard]] std::size_t thousandths_ratio_check_local_first() const noexcept {
        return thousandths_ratio_check_local_first_;
    }

    void thousandths_ratio_check_local_first(std::size_t arg) noexcept {
        thousandths_ratio_check_local_first_ = arg;
    }
private:
    std::size_t threads_{};
    bool set_core_affinity_{};
    std::size_t initial_core_{};
    bool assign_numa_nodes_uniformly_{};
    std::size_t randomize_memory_usage_{};
    std::size_t force_numa_node_{};
    bool stealing_enabled_{};
    bool use_preferred_worker_for_current_thread_{};
    std::size_t stealing_wait_{};
    std::size_t task_polling_wait_{};
    bool busy_worker_{};
    std::size_t watcher_interval_{};
    std::size_t worker_try_count_{};
    std::size_t worker_suspend_timeout_{};
    std::size_t thousandths_ratio_check_local_first_{};
};

} // namespace
