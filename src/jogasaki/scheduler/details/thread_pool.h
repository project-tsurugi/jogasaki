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

#include <vector>

#include <boost/asio.hpp>
#include <boost/thread.hpp>

#include <jogasaki/utils/interference_size.h>
#include <jogasaki/scheduler/thread_params.h>
#include "thread.h"

namespace jogasaki::scheduler::details {

/**
 * @brief simple implementation of fixed size thread pool
 */
class cache_align thread_pool {
public:
    /**
     * @brief create default object with single thread
     */
    thread_pool();;

    thread_pool(thread_pool const& other) = delete;
    thread_pool& operator=(thread_pool const& other) = delete;
    thread_pool(thread_pool&& other) noexcept = delete;
    thread_pool& operator=(thread_pool&& other) noexcept = delete;

    /**
     * @brief create new object
     * @param params thread configuration parameters
     */
    explicit thread_pool(thread_params params);

    /**
     * @brief destroy the object stopping all running threads
     */
    ~thread_pool() noexcept;

    /**
     * @brief join all the running threads
     */
    void join();

    /**
     * @brief submit task for schedule
     * @tparam F task type to schedule
     * @param f the task to schedule
     */
    template <class F>
    void submit(F&& f) {
        io_service_.post(std::forward<F>(f));
    }

    void start();

    void stop();

private:
    std::size_t max_threads_{};
    boost::asio::io_service io_service_{};
    std::vector<thread> threads_{};
    boost::thread_group thread_group_{};
    std::unique_ptr<boost::asio::io_service::work> work_{}; // work to keep service running
    bool set_core_affinity_;
    std::size_t initial_core_{};
    bool assign_numa_nodes_uniformly_{};
    std::size_t randomize_memory_usage_{};
    bool started_{false};

    void prepare_threads_();
    void cleanup_threads_();
};

}



