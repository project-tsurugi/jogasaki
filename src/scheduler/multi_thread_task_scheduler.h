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

#include <glog/logging.h>
#include <boost/asio.hpp>
#include <boost/thread.hpp>

#include <model/task.h>
#include <channel.h>
#include <executor/common/task.h>
#include <utils.h>
#include "task_scheduler.h"
#include "thread_params.h"

namespace jogasaki::scheduler {

/**
 * @brief simple implementation of fixed size thread pool
 */
class thread_pool {
public:
    thread_pool() : thread_pool(thread_params(1)) {};
    thread_pool(thread_pool const& other) = delete;
    thread_pool& operator=(thread_pool const& other) = delete;
    thread_pool(thread_pool&& other) noexcept = delete;
    thread_pool& operator=(thread_pool&& other) noexcept = delete;
    explicit thread_pool(thread_params params) :
            threads_(params.threads()),
            io_service_(),
            work_(std::make_unique<boost::asio::io_service::work>(io_service_)),
            set_core_affinity_(params.is_set_core_affinity()),
            initial_core_(params.inititial_core()) {
        prepare_threads_();
    }

    ~thread_pool() noexcept {
        try {
            join();
            io_service_.stop();
        } catch (...) {
            LOG(ERROR) << "error on finishing thread pool";
        }
    }

    void join() {
        work_.reset();
        thread_group_.join_all();
    }

    template <class F>
    void submit(F&& f) {
        io_service_.post(std::forward<F>(f));
    }

private:
    std::size_t threads_{};
    boost::asio::io_service io_service_{};
    boost::thread_group thread_group_{};
    std::unique_ptr<boost::asio::io_service::work> work_{}; // work to keep service running
    bool set_core_affinity_;
    std::size_t initial_core_{};

    void prepare_threads_() {
        for(std::size_t i = 0; i < threads_; ++i) {
            auto thread = new boost::thread([this]() {
                io_service_.run();
            });
            if(set_core_affinity_) {
                set_core_affinity(thread, i+initial_core_);
            }
            thread_group_.add_thread(thread);
        }
    }
};

/*
 * @brief task scheduler who is responsible for running task concurrently and efficiently
 */
class multi_thread_task_scheduler : public task_scheduler {
public:
    multi_thread_task_scheduler() = default;
    ~multi_thread_task_scheduler() override = default;
    multi_thread_task_scheduler(multi_thread_task_scheduler const& other) = delete;
    multi_thread_task_scheduler& operator=(multi_thread_task_scheduler const& other) = delete;
    multi_thread_task_scheduler(multi_thread_task_scheduler&& other) noexcept = delete;
    multi_thread_task_scheduler& operator=(multi_thread_task_scheduler&& other) noexcept = delete;
    explicit multi_thread_task_scheduler(thread_params params) :
            threads_(params) {}

private:
    /**
     * @brief task wrapper to run the task continuously while task result is 'proceed'
     */
    class proceeding_task_wrapper {
    public:
        explicit proceeding_task_wrapper(model::task* original) : original_(original) {}
        void operator()() {
            while(original_->operator()() == model::task_result::proceed) {}
        }
    private:
        model::task* original_{};
    };

public:
    void schedule_task(model::task* t) override {
        threads_.submit(proceeding_task_wrapper(t));
        tasks_.emplace(t);
    }

    void run() override {
        // no-op - tasks are already running on threads
    }
    void stop() override {
        threads_.join();
    }
    void remove_task(model::task* t) override {
        tasks_.erase(t);
    }
private:
    std::unordered_set<model::task*> tasks_{};
    thread_pool threads_{};
};

}



