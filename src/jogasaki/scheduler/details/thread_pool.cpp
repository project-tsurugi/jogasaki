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
#include "thread_pool.h"

#include <glog/logging.h>
#include <boost/asio.hpp>

#include <jogasaki/model/task.h>
#include <jogasaki/executor/common/task.h>
#include <jogasaki/scheduler/thread_params.h>
#include <jogasaki/utils/core_affinity.h>

namespace jogasaki::scheduler::details {

thread_pool::thread_pool() : thread_pool(thread_params(1)) {}

thread_pool::thread_pool(thread_params params) :
    max_threads_(params.threads()),
    io_service_(),
    work_(std::make_unique<boost::asio::io_service::work>(io_service_)),
    set_core_affinity_(params.is_set_core_affinity()),
    initial_core_(params.inititial_core()),
    assign_numa_nodes_uniformly_(params.assign_numa_nodes_uniformly()),
    randomize_memory_usage_(params.randomize_memory_usage()),
    force_numa_node_(params.force_numa_node())
{
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
    if(set_core_affinity_) {
        utils::thread_core_affinity(0);
    }
    prepare_threads();
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
    cleanup_threads();
    assert(thread_group_.size() == 0); //NOLINT
    started_ = false;
}

void thread_pool::prepare_threads() {
    threads_.reserve(max_threads_);
    for(std::size_t i = 0; i < max_threads_; ++i) {
        auto& thread = threads_.emplace_back();
        auto core = i+initial_core_;
        thread([this, &thread, core]() {
            if(set_core_affinity_ || force_numa_node_ != thread_params::numa_node_unspecified) {
                utils::thread_core_affinity(core, assign_numa_nodes_uniformly_, force_numa_node_);
            }
            if (randomize_memory_usage_ != 0) {
                thread.allocate_randomly(randomize_memory_usage_);
            }
            io_service_.run();
        });
        thread_group_.add_thread(thread.get());
    }
}

void thread_pool::cleanup_threads() {
    for(auto& t : threads_) {
        thread_group_.remove_thread(t.get());
        t.reset();
    }
}

}



