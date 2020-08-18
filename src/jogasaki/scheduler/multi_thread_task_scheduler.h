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

#include <unordered_set>
#include <random>

#include <glog/logging.h>
#include <boost/asio.hpp>
#include <boost/thread.hpp>

#include <jogasaki/utils/core_affinity.h>
#include <jogasaki/model/task.h>
#include <jogasaki/executor/common/task.h>
#include <jogasaki/utils/random.h>
#include "task_scheduler.h"
#include "thread_params.h"

namespace jogasaki::scheduler {

class thread {
public:
    thread() = default;

    template <class T>
    void start(T func) {
        entity_ = std::make_unique<boost::thread>(std::forward<T>(func));
    }

    [[nodiscard]] boost::thread* get() const noexcept {
        return entity_.get();
    }
    void reset() noexcept {
        entity_.reset();
        for(auto&& e : randomized_buffer_) {
            e.reset();
        }
    }

    void allocate_randomly() {
        static constexpr std::array<std::size_t, 14> sizes =
            {8, 16, 160, 320, 640, 1280, 2560, 5120, 10240, 16*1024, 20*1024, 40*1024, 80*1024, 160*1024 };
        utils::xorshift_random64 rnd(std::random_device{}());
        std::stringstream ss{};
        ss << "random allocation : ";
        std::size_t total = 0;
        for(auto sz : sizes) {
            std::size_t count = rnd() % 13;
            for(std::size_t i=0; i < count; ++i) {
                randomized_buffer_.emplace_back(std::make_unique<char[]>(sz)); //NOLINT
            }
            ss << "[" << sz << "]*" << count << " ";
            total += sz * count;
        }
        ss << "total: " << total;
        VLOG(2) << ss.str();
    }

private:
    std::unique_ptr<boost::thread> entity_{};
    std::vector<std::unique_ptr<char[]>> randomized_buffer_{}; //NOLINT
};
/**
 * @brief simple implementation of fixed size thread pool
 */
class thread_pool {
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
    bool randomize_memory_usage_{};
    bool started_{false};

    void prepare_threads_();
    void cleanup_threads_();
};

/*
 * @brief task scheduler using multiple threads
 */
class multi_thread_task_scheduler : public task_scheduler {
public:
    multi_thread_task_scheduler() = default;
    ~multi_thread_task_scheduler() override = default;
    multi_thread_task_scheduler(multi_thread_task_scheduler const& other) = delete;
    multi_thread_task_scheduler& operator=(multi_thread_task_scheduler const& other) = delete;
    multi_thread_task_scheduler(multi_thread_task_scheduler&& other) noexcept = delete;
    multi_thread_task_scheduler& operator=(multi_thread_task_scheduler&& other) noexcept = delete;
    explicit multi_thread_task_scheduler(thread_params params);

private:
    /**
     * @brief task wrapper to run the task continuously while task result is 'proceed'
     */
    class proceeding_task_wrapper {
    public:
        explicit proceeding_task_wrapper(std::weak_ptr<model::task> original);

        void operator()();
    private:
        std::weak_ptr<model::task> original_{};
    };

public:
    void schedule_task(std::shared_ptr<model::task> const& t) override;

    void wait_for_progress() override;

    void start() override;

    void stop() override;

    [[nodiscard]] task_scheduler_kind kind() const noexcept override;
private:
    std::unordered_map<model::task::identity_type, std::weak_ptr<model::task>> tasks_{};
    thread_pool threads_{};
};

}



