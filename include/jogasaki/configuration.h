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

#include <iomanip>
#include <cstddef>

namespace jogasaki {

/**
 * @brief database environment global configuration
 * @details getters specified with const are thread safe
 */
class configuration {
public:
    static constexpr std::size_t numa_node_unspecified = static_cast<std::size_t>(-1);

    [[nodiscard]] bool single_thread() const noexcept {
        return single_thread_task_scheduler_;
    }

    void single_thread(bool arg) noexcept {
        single_thread_task_scheduler_ = arg;
    }

    [[nodiscard]] std::size_t thread_pool_size() const noexcept {
        return thread_pool_size_;
    }

    void thread_pool_size(std::size_t arg) noexcept {
        thread_pool_size_ = arg;
    }

    [[nodiscard]] std::size_t default_partitions() const noexcept {
        return default_process_partitions_;
    }

    void default_partitions(std::size_t arg) noexcept {
        default_process_partitions_ = arg;
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

    [[nodiscard]] bool use_sorted_vector() const noexcept {
        return use_sorted_vector_reader_;
    }

    void use_sorted_vector(bool arg) noexcept {
        use_sorted_vector_reader_ = arg;
    }

    [[nodiscard]] bool noop_pregroup() const {
        return noop_pregroup_;
    }

    void noop_pregroup(bool arg) noexcept {
        noop_pregroup_ = arg;
    }

    [[nodiscard]] bool assign_numa_nodes_uniformly() const noexcept {
        return assign_numa_nodes_uniformly_;
    }

    void assign_numa_nodes_uniformly(bool arg) noexcept {
        assign_numa_nodes_uniformly_ = arg;
    }

    [[nodiscard]] std::size_t randomize_memory_usage() const noexcept {
        return randomize_memory_usage_;
    }

    void randomize_memory_usage(std::size_t arg) noexcept {
        randomize_memory_usage_ = arg;
    }

    [[nodiscard]] std::size_t force_numa_node() const noexcept {
        return force_numa_node_;
    }

    void force_numa_node(std::size_t arg) noexcept {
        force_numa_node_ = arg;
    }

    [[nodiscard]] bool prepare_benchmark_tables() const noexcept {
        return prepare_benchmark_tables_;
    }

    void prepare_benchmark_tables(bool arg) noexcept {
        prepare_benchmark_tables_ = arg;
    }

    [[nodiscard]] bool prepare_analytics_benchmark_tables() const noexcept {
        return prepare_analytics_benchmark_tables_;
    }

    void prepare_analytics_benchmark_tables(bool arg) noexcept {
        prepare_analytics_benchmark_tables_ = arg;
    }

    [[nodiscard]] bool stealing_enabled() const noexcept {
        return stealing_enabled_;
    }

    void stealing_enabled(bool arg) noexcept {
        stealing_enabled_ = arg;
    }

    [[nodiscard]] bool work_sharing() const noexcept {
        return work_sharing_;
    }

    void work_sharing(bool arg) noexcept {
        work_sharing_ = arg;
    }

    [[nodiscard]] std::string_view db_location() const noexcept {
        return db_location_;
    }

    void db_location(std::string_view arg) noexcept {
        db_location_ = arg;
    }

    [[nodiscard]] bool respect_client_core() const noexcept {
        return respect_client_core_;
    }

    void respect_client_core(bool arg) noexcept {
        respect_client_core_ = arg;
    }

    friend inline std::ostream& operator<<(std::ostream& out, configuration const& cfg) {
        return out << std::boolalpha <<
            "single_thread:" << cfg.single_thread() << " " <<
            "thread_pool_size:" << cfg.thread_pool_size() << " " <<
            "default_partitions:" << cfg.default_partitions() << " " <<
            "core_affinity:" << cfg.core_affinity() << " " <<
            "initial_core:" << cfg.initial_core() << " " <<
            "assign_numa_nodes_uniformly:" << cfg.assign_numa_nodes_uniformly() << " " <<
            "force_numa_node:" << (cfg.force_numa_node() == numa_node_unspecified ? "unspecified" : std::to_string(cfg.force_numa_node())) << " " <<
            "stealing_enabled:" << cfg.stealing_enabled() << " " <<
            "prepare_benchmark_tables:" << cfg.prepare_benchmark_tables() << " " <<
            "prepare_analytics_benchmark_tables:" << cfg.prepare_analytics_benchmark_tables() << " " <<
            "";
    }

private:
    bool single_thread_task_scheduler_ = false;
    std::size_t thread_pool_size_ = 5;
    std::size_t default_process_partitions_ = 5;
    bool set_core_affinity_ = false;
    std::size_t initial_core_ = 1;
    bool use_sorted_vector_reader_ = false;
    bool noop_pregroup_ = false;
    bool assign_numa_nodes_uniformly_ = false;
    std::size_t randomize_memory_usage_ = 0;
    std::size_t force_numa_node_ = numa_node_unspecified;
    bool prepare_benchmark_tables_ = false;
    bool prepare_analytics_benchmark_tables_ = false;
    bool stealing_enabled_ = false;
    bool work_sharing_ = false;
    std::string db_location_{};
    bool respect_client_core_ = false;
};

}

