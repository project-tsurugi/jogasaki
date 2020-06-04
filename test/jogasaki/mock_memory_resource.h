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

#include <vector>
#include <memory>
#include <boost/container/pmr/monotonic_buffer_resource.hpp>

#include <jogasaki/memory/paged_memory_resource.h>

namespace jogasaki {

class mock_memory_resource : public memory::paged_memory_resource {
public:
    using monotonic_buffer_resource = boost::container::pmr::monotonic_buffer_resource;
    explicit mock_memory_resource(std::size_t max_bytes = 0, std::size_t max_allocations = 0) :
            max_bytes_(max_bytes), max_allocations_(max_allocations)
    {
        resources_.emplace_back(std::make_unique<monotonic_buffer_resource>());
    }

protected:
    void * do_allocate(std::size_t bytes, std::size_t alignment) override {
        total_bytes_allocated_ += bytes;
        if ((max_bytes_ != 0 && max_bytes_ < allocated_bytes_on_current_page_ + bytes) ||
                        (max_allocations_ != 0 && max_allocations_ < allocations_on_current_page_ + 1)) {
            resources_.emplace_back(std::make_unique<monotonic_buffer_resource>());
            allocated_bytes_on_current_page_ = 0;
            allocations_on_current_page_ = 0;
        }
        ++allocations_on_current_page_;
        allocated_bytes_on_current_page_ += bytes;
        return resources_.back()->allocate(bytes, alignment);
    }
    void do_deallocate(void *p, std::size_t bytes, std::size_t alignment) override {
        total_bytes_allocated_ -= bytes;
        return resources_.back()->deallocate(p, bytes, alignment);
    }
    bool do_is_equal(const memory_resource &other) const noexcept override {
        return is_equal(other);
    }
    std::size_t do_page_remaining(std::size_t) const noexcept override {
        return unknown_size;
    }
public:
    void end_current_page() override {
        resources_.emplace_back(std::make_unique<monotonic_buffer_resource>());
        allocated_bytes_on_current_page_ = 0;
        allocations_on_current_page_ = 0;
    }

    std::size_t total_bytes_allocated_ = 0;
    std::vector<std::unique_ptr<monotonic_buffer_resource>> resources_{};
    std::size_t max_bytes_{};
    std::size_t max_allocations_{};
    std::size_t allocated_bytes_on_current_page_{};
    std::size_t allocations_on_current_page_{};
};

}
