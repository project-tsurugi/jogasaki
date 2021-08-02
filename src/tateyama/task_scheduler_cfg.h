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

#include <cstddef>
#include <iomanip>

namespace tateyama {

/**
 * @brief task scheduler configuration
 * @details getters specified with const are thread safe
 */
class task_scheduler_cfg {
public:
    static constexpr std::size_t numa_node_unspecified = static_cast<std::size_t>(-1);

    [[nodiscard]] std::size_t thread_count() const noexcept {
        return thread_count_;
    }

    void thread_count(std::size_t arg) noexcept {
        thread_count_ = arg;
    }

    [[nodiscard]] bool core_affinity() const noexcept {
        return set_core_affinity_;
    }

    void core_affinity(bool arg) noexcept {
        set_core_affinity_ = arg;
    }

    [[nodiscard]] std::size_t initial_core() const noexcept {
        return initial_core_;
    }

    void initial_core(std::size_t arg) noexcept {
        initial_core_ = arg;
    }

    [[nodiscard]] bool assign_numa_nodes_uniformly() const noexcept {
        return assign_numa_nodes_uniformly_;
    }

    void assign_numa_nodes_uniformly(bool arg) noexcept {
        assign_numa_nodes_uniformly_ = arg;
    }

    [[nodiscard]] std::size_t force_numa_node() const noexcept {
        return force_numa_node_;
    }

    void force_numa_node(std::size_t arg) noexcept {
        force_numa_node_ = arg;
    }

    [[nodiscard]] bool stealing_enabled() const noexcept {
        return stealing_enabled_;
    }

    void stealing_enabled(bool arg) noexcept {
        stealing_enabled_ = arg;
    }

    [[nodiscard]] bool round_robbin() const noexcept {
        return round_robbin_;
    }

    void round_robbin(bool arg) noexcept {
        round_robbin_ = arg;
    }
    
    friend inline std::ostream& operator<<(std::ostream& out, task_scheduler_cfg const& cfg) {
        return out << std::boolalpha <<
            "thread_count:" << cfg.thread_count() << " " <<
            "set_core_affinity:" << cfg.core_affinity() << " " <<
            "initial_core:" << cfg.initial_core() << " " <<
            "assign_numa_nodes_uniformly:" << cfg.assign_numa_nodes_uniformly() << " " <<
            "force_numa_node:" << (cfg.force_numa_node() == numa_node_unspecified ? "unspecified" : std::to_string(cfg.force_numa_node())) << " " <<
            "stealing_enabled:" << cfg.stealing_enabled() << " " <<
            "round_robbin:" << cfg.round_robbin() << " " <<
            "";
    }
    
private:
    std::size_t thread_count_ = 5;
    bool set_core_affinity_ = true;
    std::size_t initial_core_ = 1;
    bool assign_numa_nodes_uniformly_ = true;
    std::size_t force_numa_node_ = numa_node_unspecified;
    bool stealing_enabled_ = false;
    bool round_robbin_ = false;
};

}

