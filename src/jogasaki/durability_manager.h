/*
 * Copyright 2018-2022 tsurugi project.
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
#include <memory>

#include <tbb/concurrent_priority_queue.h>

#include <jogasaki/transaction_context.h>

namespace jogasaki {

namespace details {

struct less {
    bool operator()(
        std::shared_ptr<transaction_context> const& a,
        std::shared_ptr<transaction_context> const& b
    ) {
        return a->durability_marker() > b->durability_marker();
    }
};

} // namespace details

/**
 * @brief durability manager
 */
class durability_manager {
public:
    using element_type = std::shared_ptr<transaction_context>;

    using durability_marker_type = transaction_context::durability_marker_type;

    using callback = std::function<void(element_type const&)>;

    durability_manager() = default;
    ~durability_manager() = default;
    durability_manager(durability_manager const& other) = default;
    durability_manager& operator=(durability_manager const& other) = default;
    durability_manager(durability_manager&& other) noexcept = default;
    durability_manager& operator=(durability_manager&& other) noexcept = default;

    durability_marker_type current_durability_marker() const;

    bool update_durability_marker(
        durability_marker_type marker,
        callback cb
    );

    void add(element_type arg);

private:
    tbb::concurrent_priority_queue<element_type, details::less> heap_{details::less{}};
    std::atomic_bool marker_set_{false};
    std::atomic<durability_marker_type> current_{};
};

}

