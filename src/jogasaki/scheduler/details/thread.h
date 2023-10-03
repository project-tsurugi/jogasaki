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

#include <thread>
#include <random>

#include <glog/logging.h>
#include <boost/thread.hpp>

#include <jogasaki/logging.h>
#include <jogasaki/utils/random.h>
#include <jogasaki/utils/interference_size.h>

namespace jogasaki::scheduler::details {

/**
 * @brief boost thread wrapper to keep the buffer together
 */
class cache_align thread {
public:
    thread() : id_(new_identity()) {};

    template <class T>
    void operator()(T&& func) {
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

    /**
     * @brief allocate memory randomly to randomize state of the arena
     * @param magnitude maximum number of allocations for each size class. The larger the
     */
    void allocate_randomly(std::size_t magnitude) {
        if (magnitude == 0) return;
        static constexpr std::array<std::size_t, 14> sizes =
            {8, 16, 160, 320, 640, 1280, 2560, 5120, 10240, 16*1024, 20*1024, 40*1024, 80*1024, 160*1024 };
        utils::xorshift_random64 rnd(54321UL + id_);
        std::stringstream ss{};
        ss << "random allocation : ";
        std::size_t total = 0;
        for(auto sz : sizes) {
            std::size_t count = rnd() % magnitude;
            for(std::size_t i=0; i < count; ++i) {
                randomized_buffer_.emplace_back(std::make_unique<char[]>(sz)); //NOLINT
            }
            ss << "[" << sz << "]*" << count << " ";
            total += sz * count;
        }
        ss << "total: " << total;
        VLOG(log_trace) << ss.str();
    }

private:
    std::unique_ptr<boost::thread> entity_{};
    std::vector<std::unique_ptr<char[]>> randomized_buffer_{}; //NOLINT
    std::size_t id_{};

    std::size_t new_identity() {
        cache_align static std::atomic<size_t> source = 0;
        return source++;
    }
};

}



