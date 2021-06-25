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
#include <variant>
#include <ios>
#include <functional>

#include <glog/logging.h>

#include <concurrentqueue/moodycamel/concurrentqueue.h>

#include "task_ref.h"
#include "cache_align.h"

namespace tateyama::impl {

template <class T>
class cache_align mc_queue {
public:
    /**
     * @brief construct empty instance
     */
    mc_queue() = default;

    void push(T const& t) {
        origin_.enqueue(t);
    }

    void push(T&& t) {
        origin_.enqueue(std::move(t));
    }

    bool try_pop(T& t) {
        return origin_.try_dequeue(t);
    }

    [[nodiscard]] std::size_t size() const {
        return origin_.size_approx();
    }

    [[nodiscard]] bool empty() const {
        return size() == 0;
    }

    void clear() {
        T value;
        while( !empty() ) {
            try_pop(value);
        }
    }
    void reconstruct() {
        origin_.~ConcurrentQueue();
        new (&origin_)moodycamel::ConcurrentQueue<T>();
    }
private:
    moodycamel::ConcurrentQueue<T> origin_{};

};

}
