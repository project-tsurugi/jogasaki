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
#include <event.h>

namespace jogasaki {

/*
 * @brief communication channel among dag/dag controller/tasks
 */
class channel {
public:
    using element_type = event;

    channel() = default;
    virtual ~channel() = default;
    channel(channel&& other) noexcept = default;
    channel& operator=(channel&& other) noexcept = default;

    template <typename ... Args>
    void emplace(Args&& ... args) {
        events_->emplace(std::forward<Args>(args)...);
    }

    void push(element_type e) {
        events_->push(std::move(e));
    }

    bool pop(element_type& e) {
        return events_->try_pop(e);
    }

private:
    std::unique_ptr<tbb::concurrent_queue<element_type>> events_ = std::make_unique<tbb::concurrent_queue<element_type>>();
};

}
