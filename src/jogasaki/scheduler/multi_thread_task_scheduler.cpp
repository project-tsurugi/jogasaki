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
#include "multi_thread_task_scheduler.h"

#include <unordered_set>

#include <glog/logging.h>
#include <boost/asio.hpp>
#include <boost/thread.hpp>

#include <jogasaki/utils/core_affinity.h>
#include <jogasaki/model/task.h>
#include <jogasaki/executor/common/task.h>
#include "task_scheduler.h"
#include "thread_params.h"

namespace jogasaki::scheduler {

thread_pool::thread_pool() : thread_pool(thread_params(1)) {}

thread_pool::thread_pool(thread_params params) :
    max_threads_(params.threads()),
    io_service_(),
    work_(std::make_unique<boost::asio::io_service::work>(io_service_)),
    set_core_affinity_(params.is_set_core_affinity()),
    initial_core_(params.inititial_core()),
    assign_numa_nodes_uniformly_(params.assign_numa_nodes_uniformly()) {
    start();
}

thread_pool::~thread_pool() noexcept {
    stop();
}

void thread_pool::join() {
    work_.reset();
    thread_group_.join_all();
}

void thread_pool::start() {
    if (started_) return;
    prepare_threads_();
    started_ = true;
}

void thread_pool::stop() {
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

void thread_pool::prepare_threads_() {
    threads_.reserve(max_threads_);
    for(std::size_t i = 0; i < max_threads_; ++i) {
        auto thread = new boost::thread([this]() {
            io_service_.run();
        });
        if(set_core_affinity_) {
            utils::set_core_affinity(thread, i+initial_core_, assign_numa_nodes_uniformly_);
        }
        thread_group_.add_thread(thread);
        threads_.emplace_back(thread);
    }
}

void thread_pool::cleanup_threads_() {
    for(auto* t : threads_) {
        thread_group_.remove_thread(t);
        delete t; //NOLINT thread_group handles raw pointer
    }
}

multi_thread_task_scheduler::multi_thread_task_scheduler(thread_params params) :
    threads_(params) {}

void multi_thread_task_scheduler::schedule_task(const std::shared_ptr<model::task> &t) {
    threads_.submit(proceeding_task_wrapper(t));
    tasks_.emplace(t->id(), t);
}

void multi_thread_task_scheduler::wait_for_progress() {
    // no-op - tasks are already running on threads
}

void multi_thread_task_scheduler::start() {
    threads_.start();
}

void multi_thread_task_scheduler::stop() {
    threads_.stop();
}

task_scheduler_kind multi_thread_task_scheduler::kind() const noexcept {
    return task_scheduler_kind::multi_thread;
}

multi_thread_task_scheduler::proceeding_task_wrapper::proceeding_task_wrapper(std::weak_ptr<model::task> original) : original_(std::move(original)) {}

void multi_thread_task_scheduler::proceeding_task_wrapper::operator()() {
    auto s = original_.lock();
    if (!s) return;
    while(s->operator()() == model::task_result::proceed) {}
}
}



