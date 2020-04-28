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

#include <boost/container/pmr/monotonic_buffer_resource.hpp>
#include <memory/paged_memory_resource.h>

namespace dc {

class mock_memory_resource : public memory::paged_memory_resource {
protected:
    void * do_allocate(std::size_t bytes, std::size_t alignment) override {
        total_ += bytes;
        return resource_.allocate(bytes, alignment);
    }
    void do_deallocate(void *p, std::size_t bytes, std::size_t alignment) override {
        total_ -= bytes;
        return resource_.deallocate(p, bytes, alignment);
    }
    bool do_is_equal(const memory_resource &other) const noexcept override {
        return is_equal(other);
    }
    std::size_t do_page_remaining(std::size_t) const noexcept override {
        return unknown_size;
    }
public:
    void end_current_page() override {}
    std::size_t total_ = 0;
    boost::container::pmr::monotonic_buffer_resource resource_{};
};

}
