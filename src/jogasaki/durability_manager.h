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
#include <memory>
#include <tbb/concurrent_priority_queue.h>

#include <jogasaki/request_context.h>

namespace jogasaki {

class request_context;

namespace details {

using durability_manager_element_type = std::shared_ptr<request_context>;

struct less {
    bool operator()(
        durability_manager_element_type const& a,
        durability_manager_element_type const& b
    ) const {
        return a->transaction()->durability_marker() > b->transaction()->durability_marker();
    }
};

}  // namespace details

/**
 * @brief durability manager
 * @details durability manager manages the current durability marker and make callback when marker is updated
 */
class durability_manager {
public:
    using element_type = details::durability_manager_element_type;

    using element_reference_type = element_type const&;

    using marker_type = transaction_context::durability_marker_type;

    using callback = std::function<void(element_reference_type)>;

    durability_manager() = default;
    ~durability_manager() = default;
    durability_manager(durability_manager const& other) = delete;
    durability_manager& operator=(durability_manager const& other) = delete;
    durability_manager(durability_manager&& other) noexcept = delete;
    durability_manager& operator=(durability_manager&& other) noexcept = delete;

    /**
     * @brief accessor to the current durability marker
    */
    [[nodiscard]] marker_type current_marker() const;

    /**
     * @brief update the durability marker and invoke callback for wait list entries
     * After the callback, the entry(transaction) is removed from the wait list.
     * In order to avoid unpredictable function duration, this function schedules tasks to dispatch work
     * to worker threads.
     * @param marker the new value for the marker
     * @param cb the callback to be called for the transaction that is made durable
    */
    bool update_current_marker(
        marker_type marker,
        callback cb
    );

    /**
     * @brief add transaction to the wait list
    */
    void add_to_waitlist(element_type arg);

    /**
     * @brief check the wait list and if it's empty, update the durability marker
     * If the wait list is not empty, this function is no-op.
     * This is convenient for quick check if wait list is empty and updating marker is trivial.
     * If this function returns true, you can omit calling `update_current_marker` because there is no
     * entry for the callback.
     * @param marker the new value for the marker
     * @return true if the wait list is empty (marker is updated)
     * @return false otherwise, the function does nothing
    */
    bool instant_update_if_waitlist_empty(marker_type marker);

    /**
     * @brief print diagnostics
     * @param os the output stream to write diagnostic info.
     */
    void print_diagnostic(std::ostream& os);

private:
    tbb::concurrent_priority_queue<element_type, details::less> heap_{details::less{}};
    std::atomic_bool current_set_{false};
    std::atomic<marker_type> current_{};
    std::atomic_bool heap_in_use_{false};
};

}  // namespace jogasaki
