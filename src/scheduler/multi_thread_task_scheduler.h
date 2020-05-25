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
    /**
     * @brief create default object with single thread
     */
    thread_pool() : thread_pool(thread_params(1)) {};

    thread_pool(thread_pool const& other) = delete;
    thread_pool& operator=(thread_pool const& other) = delete;
    thread_pool(thread_pool&& other) noexcept = delete;
    thread_pool& operator=(thread_pool&& other) noexcept = delete;

    /**
     * @brief create new object
     * @param params thread configuration parameters
     */
    explicit thread_pool(thread_params params) :
            max_threads_(params.threads()),
            io_service_(),
            work_(std::make_unique<boost::asio::io_service::work>(io_service_)),
            set_core_affinity_(params.is_set_core_affinity()),
            initial_core_(params.inititial_core()),
            assign_nume_nodes_uniformly_(params.assign_nume_nodes_uniformly()) {
        start();
    }

    /**
     * @brief destroy the object stopping all running threads
     */
    ~thread_pool() noexcept {
        stop();
    }

    /**
     * @brief join all the running threads
     */
    void join() {
        work_.reset();
        thread_group_.join_all();
    }

    /**
     * @brief submit task for schedule
     * @tparam F task type to schedule
     * @param f the task to schedule
     */
    template <class F>
    void submit(F&& f) {
        io_service_.post(std::forward<F>(f));
    }

    void start() {
        if (started_) return;
        prepare_threads_();
        started_ = true;
    }

    void stop() {
        if (!started_) return;
        try {
            join();
            io_service_.stop();
        } catch (...) {
            LOG(ERROR) << "error on finishing thread pool";
        }
        cleanup_threads_();
        assert(thread_group_.size() == 0); //NOLINT
        started_ = false;
    }

private:
    std::size_t max_threads_{};
    boost::asio::io_service io_service_{};
    std::vector<boost::thread*> threads_{};
    boost::thread_group thread_group_{};
    std::unique_ptr<boost::asio::io_service::work> work_{}; // work to keep service running
    bool set_core_affinity_;
    std::size_t initial_core_{};
    bool assign_nume_nodes_uniformly_{};
    bool started_{false};

    void prepare_threads_() {
        threads_.reserve(max_threads_);
        for(std::size_t i = 0; i < max_threads_; ++i) {
            auto thread = new boost::thread([this]() {
                io_service_.run();
            });
            if(set_core_affinity_) {
                set_core_affinity(thread, i+initial_core_, assign_nume_nodes_uniformly_);
            }
            thread_group_.add_thread(thread);
            threads_.emplace_back(thread);
        }
    }

    void cleanup_threads_() {
        for(auto* t : threads_) {
            thread_group_.remove_thread(t);
            delete t; //NOLINT thread_group handles raw pointer
        }
    }
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
    explicit multi_thread_task_scheduler(thread_params params) :
            threads_(params) {}

private:
    /**
     * @brief task wrapper to run the task continuously while task result is 'proceed'
     */
    class proceeding_task_wrapper {
    public:
        explicit proceeding_task_wrapper(std::weak_ptr<model::task> original) : original_(std::move(original)) {}

        void operator()() {
            auto s = original_.lock();
            if (!s) return;
            while(s->operator()() == model::task_result::proceed) {}
        }
    private:
        std::weak_ptr<model::task> original_{};
    };

public:
    void schedule_task(std::shared_ptr<model::task> const& t) override {
        threads_.submit(proceeding_task_wrapper(t));
        tasks_.emplace(t->id(), t);
    }

    void wait_for_progress() override {
        // no-op - tasks are already running on threads
    }

    void start() override {
        threads_.start();
    }

    void stop() override {
        threads_.stop();
    }

    [[nodiscard]] task_scheduler_kind kind() const noexcept override {
        return task_scheduler_kind::multi_thread;
    }
private:
    std::unordered_map<model::task::identity_type, std::weak_ptr<model::task>> tasks_{};
    thread_pool threads_{};
};

}



