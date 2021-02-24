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

#include <tbb/concurrent_queue.h>

#include <jogasaki/event.h>

namespace jogasaki {

using blocking_queue_type = tbb::concurrent_bounded_queue<event>;
using non_blocking_queue_type = tbb::concurrent_queue<event>;

/*
 * @brief communication channel among dag/dag controller/tasks
 */
template <class T>
class basic_channel {
public:
    using queue_type = T;

    /**
     * @brief create new object
     */
    basic_channel() = default;

    /**
     * @brief create new object
     */
    explicit basic_channel(bool non_blocking) :
        non_blocking_(non_blocking)
    {}

    /**
     * @brief destroy object
     */
    ~basic_channel() = default;

    basic_channel(basic_channel const& other) = delete;
    basic_channel& operator=(basic_channel const& other) = delete;
    basic_channel(basic_channel&& other) noexcept = delete;
    basic_channel& operator=(basic_channel&& other) noexcept = delete;

    /**
     * @brief create and push new event in-place
     * @tparam Args event arguments types
     * @param args event arguments
     */
    template <typename ... Args>
    void emplace(Args&& ... args) {
        events_->emplace(std::forward<Args>(args)...);  //NOLINT // clang tidy analysis failure
    }
    /**
     * @brief push event to the queue by creating copy
     * @param e the event to be copied and pushed
     */
    void push(event e) {
        if(closed_) {
            return;
        }
        events_->push(e);
    }

    /**
     * @brief pop event from the queue
     * @param e the event reference to retrieve the popped one by copying.
     * The result is valid only if return value is true.
     * @return true if pop is successful.
     * @return false if no entry became available during the wait duration.
     */
    [[nodiscard]] bool pop(event & e) {
        if(closed_) {
            return false;
        }
        if(non_blocking_) {
            return events_->try_pop(e);
        }
        if constexpr(std::is_same_v<blocking_queue_type, queue_type>) {  //NOLINT
            try {
                events_->pop(e);
            } catch (tbb::user_abort& e) {
                // channel closed
                return false;
            }
            return true;
        }
        return events_->try_pop(e);
    }

    /**
     * @brief close the channel and unblock pending requests
     */
    void close() {
        if(closed_) {
            return;
        }
        if constexpr(std::is_same_v<blocking_queue_type, queue_type>) { //NOLINT
            events_->abort();
        }
        closed_ = true;
    }

private:
    bool non_blocking_{true};
    std::unique_ptr<queue_type> events_ = std::make_unique<queue_type>();
    bool closed_{false};
};

#ifdef USE_BLOCKING_EVENT_QUEUE
using event_channel = basic_channel<blocking_queue_type>;
#else
using event_channel = basic_channel<non_blocking_queue_type>;
#endif

}
