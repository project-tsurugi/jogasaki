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

#include <cstdint>
#include <functional>
#include <condition_variable>

#include <boost/thread.hpp>
#include <glog/logging.h>
#include <numa.h>

#include <tateyama/task_scheduler_cfg.h>
#include "core_affinity.h"
#include "cache_align.h"
#include "utils.h"
#include <tateyama/common.h>

namespace tateyama::impl {

// separating mutex and cv from thread in order to make thread movable
struct cache_align cv {
    std::condition_variable cv_{};
    std::mutex mutex_{};
};

/**
 * @brief physical thread control object
 */
class cache_align thread_control {
public:
    constexpr static std::size_t undefined = static_cast<std::size_t>(-1);
    /**
     * @brief construct empty instance
     */
    thread_control() = default;
    ~thread_control() = default;
    thread_control(thread_control const& other) = delete;
    thread_control& operator=(thread_control const& other) = delete;
    thread_control(thread_control&& other) noexcept = default;
    thread_control& operator=(thread_control&& other) noexcept = default;

    template <class F, class ...Args, class = std::enable_if_t<std::is_invocable_v<F, Args...>>>
    explicit thread_control(std::size_t thread_id, task_scheduler_cfg const* cfg, F&& callable, Args&&...args) :
        origin_(create_thread_body(thread_id, cfg, std::forward<F>(callable), std::forward<Args>(args)...))
    {}

    template <class F, class ...Args, class = std::enable_if_t<std::is_invocable_v<F, Args...>>>
    explicit thread_control(F&& callable, Args&&...args) :
        thread_control(undefined, nullptr, std::forward<F>(callable), std::forward<Args>(args)...)
    {}

    void join() {
        origin_.join();
        assert(! active_);  //NOLINT
    }

    [[nodiscard]] bool active() const noexcept {
        std::unique_lock lk{sleep_cv_->mutex_};
        return active_;
    }

    void activate() noexcept {
        {
            std::unique_lock lk{sleep_cv_->mutex_};
            active_ = true;
        }
        sleep_cv_->cv_.notify_all();
    }

    void suspend() noexcept {
        std::unique_lock lk{sleep_cv_->mutex_};
        active_ = false;
        sleep_cv_->cv_.wait(lk, [this]() {
            return active_;
        });
    }
private:
    std::unique_ptr<cv> sleep_cv_{std::make_unique<cv>()};
    bool active_{};

    // thread must come last since the construction starts new thread, which accesses the member variables above.
    boost::thread origin_{};

    template <class F, class ...Args, class = std::enable_if_t<std::is_invocable_v<F, Args...>>>
    auto create_thread_body(std::size_t thread_id, task_scheduler_cfg const* cfg, F&& callable, Args&&...args) {
        // libnuma initialize some static variables on the first call numa_node_of_cpu(). To avoid multiple threads race initialization, call it here.
        numa_node_of_cpu(sched_getcpu());

        // C++20 supports forwarding captured parameter packs. Use forward as tuple for now.
        return [=, args=std::tuple<Args...>(std::forward<Args>(args)...)]() mutable { // assuming args are copyable
            trace_scope;
            setup_core_affinity(thread_id, cfg);
            if constexpr (has_init_v<F>) {
                callable.init(thread_id);
            }
            {
                std::unique_lock lk{sleep_cv_->mutex_};
                sleep_cv_->cv_.wait(lk, [&] {
                    return active_;
                });
            }
            DLOG(INFO) << "thread " << thread_id
                << " runs on cpu:" << sched_getcpu()
                << " node:" << numa_node_of_cpu(sched_getcpu());
            std::apply([&callable](auto&& ...args) {
                callable(args...);
            }, std::move(args));
            {
                std::unique_lock lk{sleep_cv_->mutex_};
                active_ = false;
            }
        };
    }
};

}
