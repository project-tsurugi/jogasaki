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
#include <shared_mutex>
#include <condition_variable>

#include <boost/thread.hpp>
#include <glog/logging.h>

#include "configuration.h"
#include "core_affinity.h"
#include "cache_align.h"

namespace tateyama::task_scheduler {

// separating mutex and cv from thread in order to make thread movable
struct cache_align suspendable_cv {
    std::condition_variable_any cv_{};
    std::shared_mutex mutex_{};
};

/**
 * @brief physical thread with activate/deactivate
 */
class cache_align suspendable_thread {
public:
    constexpr static std::size_t undefined = static_cast<std::size_t>(-1);
    /**
     * @brief construct empty instance
     */
    suspendable_thread() = default;
    ~suspendable_thread() = default;
    suspendable_thread(suspendable_thread const& other) = delete;
    suspendable_thread& operator=(suspendable_thread const& other) = delete;
    suspendable_thread(suspendable_thread&& other) noexcept = default;
    suspendable_thread& operator=(suspendable_thread&& other) noexcept = default;

    template <class F, class ...Args, class = std::enable_if_t<std::is_invocable_v<F, Args...>>>
    explicit suspendable_thread(std::size_t thread_id, configuration const* cfg, F&& callable, Args&&...args) :
        origin_(create_thread_body(thread_id, cfg, std::forward<F>(callable), std::forward<Args>(args)...))
    {}

    template <class F, class ...Args, class = std::enable_if_t<std::is_invocable_v<F, Args...>>>
    explicit suspendable_thread(F&& callable, Args&&...args) :
        suspendable_thread(undefined, nullptr, std::forward<F>(callable), std::forward<Args>(args)...)
    {}

    void join() {
        origin_.join();
        assert(! active_ || exiting_->load());  //NOLINT
    }

    [[nodiscard]] bool active() const noexcept {
        std::shared_lock lk{sleep_cv_->mutex_};
        return active_;
    }

    void activate() noexcept {
        {
            std::unique_lock lk{sleep_cv_->mutex_};
            active_ = true;
        }
        sleep_cv_->cv_.notify_all();
    }

    void set_exiting() noexcept {
        exiting_->store(true);
    }

    void suspend() noexcept {
        assert(boost::this_thread::get_id() == origin_.get_id());  //NOLINT
        if (exiting_->load()) return;
        std::unique_lock lk{sleep_cv_->mutex_};
        active_ = false;
        sleep_cv_->cv_.wait(lk, [this]() {
            return active_;
        });
    }

private:
    std::unique_ptr<suspendable_cv> sleep_cv_{std::make_unique<suspendable_cv>()};
    boost::thread origin_{};
    bool active_{};
    std::unique_ptr<std::atomic_bool> exiting_{std::make_unique<std::atomic_bool>(false)};

    template <class F, class ...Args, class = std::enable_if_t<std::is_invocable_v<F, Args...>>>
    auto create_thread_body(std::size_t thread_id, configuration const* cfg, F&& callable, Args&&...args) {
        // C++20 supports forwarding captured parameter packs. Use forward as tuple for now.
        return [=, args=std::tuple<Args...>(std::forward<Args>(args)...)]() mutable { // assuming args are copyable
            setup_core_affinity(thread_id, cfg);
            {
                std::shared_lock lk{sleep_cv_->mutex_};
                sleep_cv_->cv_.wait(lk, [&] {
                    return active_;
                });
            }
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
