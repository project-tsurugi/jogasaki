/*
 * Copyright 2018-2023 Project Tsurugi.
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

#include <jogasaki/request_cancel_config.h>

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

    void update_skips_deletion(bool arg) noexcept {
        update_skips_deletion_ = arg;
    }

    [[nodiscard]] bool update_skips_deletion() const noexcept {
        return update_skips_deletion_;
    }

    void profile_commits(bool arg) noexcept {
        profile_commits_ = arg;
    }

    [[nodiscard]] bool profile_commits() const noexcept {
        return profile_commits_;
    }
    void skip_smv_check(bool arg) noexcept {
        skip_smv_check_ = arg;
    }

    [[nodiscard]] bool skip_smv_check() const noexcept {
        return skip_smv_check_;
    }

    void return_os_pages(bool arg) noexcept {
        return_os_pages_ = arg;
    }

    [[nodiscard]] bool return_os_pages() const noexcept {
        return return_os_pages_;
    }

    void omit_task_when_idle(bool arg) noexcept {
        omit_task_when_idle_ = arg;
    }

    [[nodiscard]] bool omit_task_when_idle() const noexcept {
        return omit_task_when_idle_;
    }

    void trace_external_log(bool arg) noexcept {
        trace_external_log_ = arg;
    }

    [[nodiscard]] bool trace_external_log() const noexcept {
        return trace_external_log_;
    }

    void plan_recording(bool arg) noexcept {
        plan_recording_ = arg;
    }

    [[nodiscard]] bool plan_recording() const noexcept {
        return plan_recording_;
    }

    void try_insert_on_upserting_secondary(bool arg) noexcept {
        try_insert_on_upserting_secondary_ = arg;
    }

    [[nodiscard]] bool try_insert_on_upserting_secondary() const noexcept {
        return try_insert_on_upserting_secondary_;
    }

    void support_boolean(bool arg) noexcept {
        support_boolean_ = arg;
    }

    [[nodiscard]] bool support_boolean() const noexcept {
        return support_boolean_;
    }

    void support_smallint(bool arg) noexcept {
        support_smallint_ = arg;
    }

    [[nodiscard]] bool support_smallint() const noexcept {
        return support_smallint_;
    }

    void scan_concurrent_operation_as_not_found(bool arg) noexcept {
        scan_concurrent_operation_as_not_found_ = arg;
    }

    [[nodiscard]] bool scan_concurrent_operation_as_not_found() const noexcept {
        return scan_concurrent_operation_as_not_found_;
    }

    void point_read_concurrent_operation_as_not_found(bool arg) noexcept {
        point_read_concurrent_operation_as_not_found_ = arg;
    }

    [[nodiscard]] bool point_read_concurrent_operation_as_not_found() const noexcept {
        return point_read_concurrent_operation_as_not_found_;
    }

    void normalize_float(bool arg) noexcept {
        normalize_float_ = arg;
    }

    [[nodiscard]] bool normalize_float() const noexcept {
        return normalize_float_;
    }

    void log_msg_user_data(bool arg) noexcept {
        log_msg_user_data_ = arg;
    }

    [[nodiscard]] bool log_msg_user_data() const noexcept {
        return log_msg_user_data_;
    }

    void req_cancel_config(std::shared_ptr<request_cancel_config> arg) noexcept {
        request_cancel_config_ = std::move(arg);
    }

    [[nodiscard]] std::shared_ptr<request_cancel_config> const& req_cancel_config() const noexcept {
        return request_cancel_config_;
    }

    void lowercase_regular_identifiers(bool arg) noexcept {
        lowercase_regular_identifiers_ = arg;
    }

    [[nodiscard]] bool lowercase_regular_identifiers() const noexcept {
        return lowercase_regular_identifiers_;
    }

    [[nodiscard]] std::size_t scan_block_size() const noexcept {
        return scan_block_size_;
    }

    void scan_block_size(std::size_t arg) noexcept {
        scan_block_size_ = arg;
    }

    [[nodiscard]] std::size_t scan_yield_interval() const noexcept {
        return scan_yield_interval_;
    }

    void scan_yield_interval(std::size_t arg) noexcept {
        scan_yield_interval_ = arg;
    }

    [[nodiscard]] std::int32_t zone_offset() const noexcept {
        return zone_offset_;
    }

    void zone_offset(std::int32_t arg) noexcept {
        zone_offset_ = arg;
    }

    [[nodiscard]] bool rtx_parallel_scan() const noexcept {
        return rtx_parallel_scan_;
    }

    void rtx_parallel_scan(bool arg) noexcept {
        rtx_parallel_scan_ = arg;
    }

    [[nodiscard]] std::size_t thousandths_ratio_check_local_first() const noexcept {
        return thousandths_ratio_check_local_first_;
    }

    void thousandths_ratio_check_local_first(std::size_t arg) noexcept {
        thousandths_ratio_check_local_first_ = arg;
    }

    [[nodiscard]] bool direct_commit_callback() const noexcept {
        return direct_commit_callback_;
    }

    void direct_commit_callback(bool arg) noexcept {
        direct_commit_callback_ = arg;
    }

    [[nodiscard]] std::size_t scan_default_parallel() const noexcept {
        return scan_default_parallel_;
    }

    void scan_default_parallel(std::size_t arg) noexcept {
        scan_default_parallel_ = arg;
    }

    [[nodiscard]] bool inplace_teardown() const noexcept {
        return inplace_teardown_;
    }

    void inplace_teardown(bool arg) noexcept {
        inplace_teardown_ = arg;
    }

    [[nodiscard]] bool inplace_dag_schedule() const noexcept {
        return inplace_dag_schedule_;
    }

    void inplace_dag_schedule(bool arg) noexcept {
        inplace_dag_schedule_ = arg;
    }

    friend inline std::ostream& operator<<(std::ostream& out, configuration const& cfg) {

        //NOLINTBEGIN
        #define print_non_default(prop)  \
            if(def.prop() != cfg.prop()) { \
                out << #prop ":" << cfg.prop() << " "; \
            }
        //NOLINTEND

        static const configuration def{};
        out << std::boolalpha;
        print_non_default(single_thread);
        print_non_default(thread_pool_size);
        print_non_default(default_partitions);
        print_non_default(core_affinity);
        print_non_default(initial_core);
        print_non_default(assign_numa_nodes_uniformly);
        print_non_default(force_numa_node);
        print_non_default(prepare_test_tables);
        print_non_default(prepare_benchmark_tables);
        print_non_default(prepare_analytics_benchmark_tables);
        print_non_default(stealing_enabled);
        print_non_default(db_location);
        print_non_default(activate_scheduler);
        print_non_default(enable_index_join);
        print_non_default(use_preferred_worker_for_current_thread);
        print_non_default(stealing_wait);
        print_non_default(task_polling_wait);
        print_non_default(lightweight_job_level);
        print_non_default(enable_hybrid_scheduler);
        print_non_default(busy_worker);
        print_non_default(watcher_interval);
        print_non_default(worker_try_count);
        print_non_default(worker_suspend_timeout);
        print_non_default(default_commit_response);
        print_non_default(update_skips_deletion);
        print_non_default(profile_commits);
        print_non_default(skip_smv_check);
        print_non_default(return_os_pages);
        print_non_default(omit_task_when_idle);
        print_non_default(trace_external_log);
        print_non_default(plan_recording);
        print_non_default(try_insert_on_upserting_secondary);
        print_non_default(support_boolean);
        print_non_default(support_smallint);
        print_non_default(scan_concurrent_operation_as_not_found);
        print_non_default(point_read_concurrent_operation_as_not_found);
        print_non_default(normalize_float);
        print_non_default(log_msg_user_data);
        print_non_default(lowercase_regular_identifiers);
        print_non_default(zone_offset);
        print_non_default(scan_block_size);
        print_non_default(scan_yield_interval);
        print_non_default(rtx_parallel_scan);
        print_non_default(thousandths_ratio_check_local_first);
        print_non_default(direct_commit_callback);
        print_non_default(scan_default_parallel);
        print_non_default(inplace_teardown);
        print_non_default(inplace_dag_schedule);

        if(cfg.req_cancel_config()) {
            out << "req_cancel_config:" << *cfg.req_cancel_config() << " "; \
        }
        return out;

        #undef print_non_default
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
    bool scheduler_rr_workers_ = false;
    bool activate_scheduler_ = true;
    bool enable_index_join_ = true;
    bool use_preferred_worker_for_current_thread_ = true;
    std::size_t stealing_wait_ = 1;
    std::size_t task_polling_wait_ = 0;
    std::size_t lightweight_job_level_ = 0;
    bool enable_hybrid_scheduler_ = true;
    bool busy_worker_ = false;
    std::size_t watcher_interval_ = 1000;
    std::size_t worker_try_count_ = 1000;
    std::size_t worker_suspend_timeout_ = 1000000;
    commit_response_kind default_commit_response_{commit_response_kind::stored};
    bool update_skips_deletion_ = false;
    bool profile_commits_ = false;
    bool skip_smv_check_ = false;
    bool return_os_pages_ = false;
    bool omit_task_when_idle_ = true;
    bool trace_external_log_ = false;
    bool plan_recording_ = true;
    bool try_insert_on_upserting_secondary_ = true;
    bool support_boolean_ = false;
    bool support_smallint_ = false;
    bool scan_concurrent_operation_as_not_found_ = true;
    bool point_read_concurrent_operation_as_not_found_ = true;
    bool normalize_float_ = true;
    bool log_msg_user_data_ = false;
    std::shared_ptr<request_cancel_config> request_cancel_config_{};
    bool lowercase_regular_identifiers_ = false;
    std::int32_t zone_offset_ = 0;
    std::size_t scan_block_size_ = 100;
    std::size_t scan_yield_interval_ = 1;
    bool rtx_parallel_scan_ = false;
    std::size_t thousandths_ratio_check_local_first_ = 100;
    bool direct_commit_callback_ = false;
    std::size_t scan_default_parallel_ = 1;
    bool inplace_teardown_ = false;
    bool inplace_dag_schedule_ = false;

};

}  // namespace jogasaki
