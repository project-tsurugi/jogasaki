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

#include <mutex>
#include <unordered_map>
#include <memory>
#include <vector>

#include <boost/thread/latch.hpp>

namespace jogasaki::utils {

using boost::latch;

class cache_align latch_set {
public:
    /**
     * @brief identifier for the point in source code
     */
    using latch_id = std::size_t;

    latch_set() noexcept = default;

    void clear() {
        std::unique_lock lock{guard_};
        latches_.clear();
        enabled_.clear();
    }

    latch& enable(latch_id loc, std::size_t count) {
        std::unique_lock lock{guard_};
        latches_[loc] = std::make_unique<latch>(count);
        enabled_.emplace(loc);
        return *latches_[loc];
    }

    bool disable(latch_id loc) {
        std::unique_lock lock{guard_};
        if (enabled_.count(loc) == 0) return false;
        return enabled_.erase(loc) > 0;
    }

    latch* get(latch_id loc) {
        std::unique_lock lock{guard_};
        if (latches_.count(loc) == 0 || enabled_.count(loc) == 0) {
            return {};
        }
        return latches_[loc].get();
    }

private:
    std::mutex guard_{};
    std::unordered_map<latch_id, std::unique_ptr<latch>> latches_{};
    std::unordered_set<latch_id> enabled_{}; // using flags because erasing a latch caused assert failure in boost::latch
};

latch_set& get_latches() {
    static std::unique_ptr<latch_set> set = std::make_unique<latch_set>();
    return *set;
}

} // namespace
