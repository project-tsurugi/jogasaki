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
        std::size_t stealing_wait = 1
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
        stealing_wait_(stealing_wait)
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
            cfg->stealing_wait()
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
};

} // namespace
