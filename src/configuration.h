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

namespace jogasaki {

/**
 * @brief database environment global configuration
 * @details getters specified with const are thread safe
 */
class configuration {
public:
    [[nodiscard]] bool single_thread() const {
        return single_thread_task_scheduler_;
    }

    void single_thread(bool arg) {
        single_thread_task_scheduler_ = arg;
    }

    [[nodiscard]] std::size_t thread_pool_size() const {
        return thread_pool_size_;
    }

    void thread_pool_size(std::size_t arg) {
        thread_pool_size_ = arg;
    }

    [[nodiscard]] std::size_t default_partitions() const {
        return default_process_partitions_;
    }

    void default_partitions(std::size_t arg) {
        default_process_partitions_ = arg;
    }

    [[nodiscard]] std::size_t default_scan_process_partitions() const {
        return default_scan_process_partitions_;
    }

    void default_scan_process_partitions(std::size_t arg) {
        default_scan_process_partitions_ = arg;
    }

    [[nodiscard]] bool core_affinity() const {
        return set_core_affinity_;
    }

    void core_affinity(bool arg) {
        set_core_affinity_ = arg;
    }

    [[nodiscard]] std::size_t initial_core() const {
        return initial_core_;
    }

    void initial_core(std::size_t arg) {
        initial_core_ = arg;
    }

    [[nodiscard]] bool use_sorted_vector() const {
        return use_sorted_vector_reader_;
    }

    void use_sorted_vector(bool arg) {
        use_sorted_vector_reader_ = arg;
    }

private:
    bool single_thread_task_scheduler_ = true;
    std::size_t thread_pool_size_ = 5;
    std::size_t default_process_partitions_ = 5;
    std::size_t default_scan_process_partitions_ = 5;
    bool set_core_affinity_ = false;
    std::size_t initial_core_ = 1;
    bool use_sorted_vector_reader_ = false;
};

}

