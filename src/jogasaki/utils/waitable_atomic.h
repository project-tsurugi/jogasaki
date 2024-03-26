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

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>

namespace jogasaki::utils {

template <class T>
class waitable_atomic {
public:

    constexpr waitable_atomic() = default;
    ~waitable_atomic() = default;
    waitable_atomic(waitable_atomic const& other) = delete;
    waitable_atomic& operator=(waitable_atomic const& other) = delete;
    waitable_atomic(waitable_atomic&& other) noexcept = delete;
    waitable_atomic& operator=(waitable_atomic&& other) noexcept = delete;

    explicit constexpr waitable_atomic(T val) noexcept :
        origin_(val)
    {}

    void store(T val) noexcept {
        origin_.store(val);
    }

    T load() const noexcept {
        return origin_.load();
    }

    waitable_atomic& operator=(T val) noexcept {
        store(val);
        return *this;
    }

    explicit operator T() const noexcept {
        return load();
    }

    void wait(T old) {
        std::unique_lock lk{mutex_};
        cv_.wait(lk, [this, &old](){
            return origin_.load() != old;
        });
    }

    void notify_one() noexcept {
        std::unique_lock lk{mutex_}; // empty lock needed to get the update on origin_
        lk.unlock();
        cv_.notify_one();
    }

    void notify_all() noexcept {
        std::unique_lock lk{mutex_}; // empty lock needed to get the update on origin_
        lk.unlock();
        cv_.notify_all();
    }

    template <class Rep, class Period>
    bool wait_for(std::chrono::duration<Rep, Period> dur, T old) {
        std::unique_lock lk{mutex_};
        return cv_.wait_for(lk, dur, [this, &old](){
            return origin_.load() != old;
        });
    }

    bool compare_exchange_weak(T& expected, T desired) noexcept {
        return origin_.compare_exchange_weak(expected, desired);
    }

    bool compare_exchange_strong(T& expected, T desired) noexcept {
        return origin_.compare_exchange_strong(expected, desired);
    }
private:
    std::atomic<T> origin_{};
    std::mutex mutex_{};
    std::condition_variable cv_{};
};

using waitable_atomic_bool = waitable_atomic<bool>;

}

