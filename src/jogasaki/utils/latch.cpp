/*
 * Copyright 2018-2024 Project Tsurugi.
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
#include "latch.h"

#include <condition_variable>
#include <mutex>

namespace jogasaki::utils {

void latch::release() {
    {
        std::unique_lock lock{guard_};
        open_ = true;
        done_ = true;
    }
    cv_.notify_all();
}

void latch::wait() {
    std::unique_lock lock{guard_};
    if (done_) return; // already released, should not wait
    open_ = false;
    cv_.wait(lock, [&](){ return open_; });
}

void latch::reset() {
    std::unique_lock lock{guard_};
    open_ = true;
    done_ = false;
}

latch::latch(bool released) noexcept:
    done_(released)
{}

} // namespace
