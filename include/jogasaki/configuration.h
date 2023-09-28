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
#include <cstdint>
#include <cstdlib>

#include "commit_response.h"

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

    [[nodiscard]] bool prepare_test_tables() const noexcept {
        return prepare_test_tables_;
    }

    void prepare_test_tables(bool arg) noexcept {
        prepare_test_tables_ = arg;
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

    [[nodiscard]] std::string_view db_location() const noexcept {
        return db_location_;
    }

    void db_location(std::string_view arg) noexcept {
        db_location_ = arg;
    }

    [[nodiscard]] bool tasked_write() const noexcept {
        return tasked_write_;
    }

    void tasked_write(bool arg) noexcept {
        tasked_write_ = arg;
    }

    [[nodiscard]] bool scheduler_rr_workers() const noexcept {
        return scheduler_rr_workers_;
    }

    void scheduler_rr_workers(bool arg) noexcept {
        scheduler_rr_workers_ = arg;
    }

    /**
     * @brief setter for activate scheduler flag
     */
    void activate_scheduler(bool arg) noexcept {
        activate_scheduler_ = arg;
    }

    /**
     * @brief accessor for activate scheduler flag
     * @return whether task scheduler should be started together with sql engine
     */
    [[nodiscard]] bool activate_scheduler() const noexcept {
        return activate_scheduler_;
    }

    /**
     * @brief setter for enable index join flag
     */
    void enable_index_join(bool arg) noexcept {
        enable_index_join_ = arg;
    }

    /**
     * @brief accessor for enable index join flag
     * @return whether index join is enabled or not
     */
    [[nodiscard]] bool enable_index_join() const noexcept {
        return enable_index_join_;
    }

    /**
     * @brief setter for use_preferred_worker_for_current_thread flag
     */
    void use_preferred_worker_for_current_thread(bool arg) noexcept {
        use_preferred_worker_for_current_thread_ = arg;
    }

    /**
     * @brief accessor for use_preferred_worker_for_current_thread flag
     * @return whether selecting preferred worker for current thread
     */
    [[nodiscard]] bool use_preferred_worker_for_current_thread() const noexcept {
        return use_preferred_worker_for_current_thread_;
    }

    /**
     * @brief accessor for stealing_wait parameter
     * @return stealing_wait parameter (coefficient for local queue check before stealing)
     */
    [[nodiscard]] std::size_t stealing_wait() const noexcept {
        return stealing_wait_;
    }

    /**
     * @brief setter for stealing_wait parameter
     */
    void stealing_wait(std::size_t arg) noexcept {
        stealing_wait_ = arg;
    }

    /**
     * @brief accessor for task_polling_wait parameter
     * @return task_polling_wait parameter (duration in micro-second before polling task queue again)
     */
    [[nodiscard]] std::size_t task_polling_wait() const noexcept {
        return task_polling_wait_;
    }

    /**
     * @brief setter for task_polling_wait parameter
     */
    void task_polling_wait(std::size_t arg) noexcept {
        task_polling_wait_ = arg;
    }

    /**
     * @brief accessor for lightweight_job_level parameter
     * @return lightweight_job_level parameter
     */
    [[nodiscard]] std::size_t lightweight_job_level() const noexcept {
        return lightweight_job_level_;
    }

    /**
     * @brief setter for lightweight_job_level parameter
     */
    void lightweight_job_level(std::size_t arg) noexcept {
        lightweight_job_level_ = arg;
    }

    /**
     * @brief setter for enable_hybrid_scheduler flag
     */
    void enable_hybrid_scheduler(bool arg) noexcept {
        enable_hybrid_scheduler_ = arg;
    }

    /**
     * @brief accessor for enable_hybrid_scheduler flag
     * @return whether serial-stealing hybrid scheduler is enabled or not
     */
    [[nodiscard]] bool enable_hybrid_scheduler() const noexcept {
        return enable_hybrid_scheduler_;
    }

    /**
     * @brief accessor for busy worker flag
     * @return whether busy worker is enabled to frequently check task queues
     * @note this is experimental feature and will be dropped soon
     */
    [[nodiscard]] bool busy_worker() const noexcept {
        return busy_worker_;
    }

    /**
     * @brief setter for busy worker flag
     * @note this is experimental feature and will be dropped soon
     */
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

    [[nodiscard]] commit_response_kind default_commit_response() const noexcept {
        return default_commit_response_;
    }

    void default_commit_response(commit_response_kind arg) noexcept {
        default_commit_response_ = arg;
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
            "prepare_test_tables:" << cfg.prepare_test_tables() << " " <<
            "prepare_benchmark_tables:" << cfg.prepare_benchmark_tables() << " " <<
            "prepare_analytics_benchmark_tables:" << cfg.prepare_analytics_benchmark_tables() << " " <<
            "stealing_enabled:" << cfg.stealing_enabled() << " " <<
            "db_location:" << cfg.db_location() << " " <<
            "tasked_write:" << cfg.tasked_write() << " " <<
            "activate_scheduler:" << cfg.activate_scheduler() << " " <<
            "enable_index_join:" << cfg.enable_index_join() << " " <<
            "use_preferred_worker_for_current_thread:" << cfg.use_preferred_worker_for_current_thread() << " " <<
            "stealing_wait:" << cfg.stealing_wait() << " " <<
            "task_polling_wait:" << cfg.task_polling_wait() << " " <<
            "lightweight_job_level:" << cfg.lightweight_job_level() << " " <<
            "enable_hybrid_scheduler:" << cfg.enable_hybrid_scheduler() << " " <<
            "busy_worker:" << cfg.busy_worker() << " " <<
            "watcher_interval:" << cfg.watcher_interval() << " " <<
            "worker_try_count:" << cfg.worker_try_count() << " " <<
            "worker_suspend_timeout:" << cfg.worker_suspend_timeout() << " " <<
            "default_commit_response:" << cfg.default_commit_response() << " " <<
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
    bool prepare_test_tables_ = false;
    bool prepare_benchmark_tables_ = false;
    bool prepare_analytics_benchmark_tables_ = false;
    bool stealing_enabled_ = true;
    std::string db_location_{};
    bool tasked_write_ = true;
    bool scheduler_rr_workers_ = false;
    bool activate_scheduler_ = true;
    bool enable_index_join_ = false;
    bool use_preferred_worker_for_current_thread_ = true;
    std::size_t stealing_wait_ = 1;
    std::size_t task_polling_wait_ = 0;
    std::size_t lightweight_job_level_ = 0;
    bool enable_hybrid_scheduler_ = true;
    bool busy_worker_ = false;
    std::size_t watcher_interval_ = 1000;
    std::size_t worker_try_count_ = 1000;
    std::size_t worker_suspend_timeout_ = 1000000;
    commit_response_kind default_commit_response_{commit_response_kind::propagated};
};

}

