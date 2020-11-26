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

#include <memory>
#include <glog/logging.h>

#include <boost/container/pmr/vector.hpp>
#include <boost/container/pmr/memory_resource.hpp>

#include <takatori/util/standard_memory_resource.h>

#include <jogasaki/executor/global.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/executor/record_writer.h>
#include <jogasaki/utils/copy_field_data.h>
#include <jogasaki/utils/interference_size.h>

namespace jogasaki::executor::process::mock {

using kind = meta::field_type_kind;

class cache_align basic_record_writer : public executor::record_writer {
public:

    using record_type = jogasaki::mock::basic_record;
    using records_type = boost::container::pmr::vector<record_type>;
    using memory_resource_type = boost::container::pmr::memory_resource;

    static constexpr std::size_t npos = static_cast<std::size_t>(-1);

    /**
     * @brief create empty object
     */
    basic_record_writer() = default;

    /**
     * @brief create new object with give metadata
     * @param meta metadata used to store the records
     */
    explicit basic_record_writer(
        maybe_shared_ptr<meta::record_meta> meta
    ) :
        meta_(std::move(meta))
    {}

    /**
     * @brief create new object
     * @param meta metadata used to store the records
     * @param capacity the number of records stored in this writer internal buffer
     * @param resource the backing memory resource for the internal buffer
     */
    basic_record_writer(
        maybe_shared_ptr<meta::record_meta> meta,
        std::size_t capacity,
        memory_resource_type* resource = takatori::util::get_standard_memory_resource()
    ) :
        meta_(std::move(meta)),
        records_(resource),
        capacity_(capacity)
    {
        records_.reserve(capacity);
    }

    /**
     * @brief write record and store internal storage as basic_record.
     * The record_meta passed to constructor is used to convert the input ref to basic_record.
     */
    bool write(accessor::record_ref rec) override {
        record_type r{rec, maybe_shared_ptr<meta::record_meta>{meta_.get()}, resource_.get()};
        if (capacity_ == npos || records_.size() < capacity_) {
            auto& x = records_.emplace_back(r);
            DVLOG(2) << x;
        } else {
            records_[pos_ % capacity_] = r;
            DVLOG(2) << records_[pos_ % capacity_];
            ++pos_;
        }
        ++write_count_;
        return false;
    }

    void flush() override {
        // no-op
    }

    void release() override {
        released_ = true;
    }

    void acquire() {
        acquired_ = true;
    }

    [[nodiscard]] std::size_t size() const noexcept {
        return std::max(write_count_, records_.size());
    }

    [[nodiscard]] records_type const& records() const noexcept {
        return records_;
    }

    [[nodiscard]] bool is_released() const noexcept {
        return released_;
    }

    [[nodiscard]] bool is_acquired() const noexcept {
        return acquired_;
    }
private:
    maybe_shared_ptr<meta::record_meta> meta_{};
    records_type records_{};
    bool released_{false};
    bool acquired_{false};
    std::size_t capacity_{npos};
    std::size_t pos_{};
    std::size_t write_count_{};
    std::unique_ptr<memory::paged_memory_resource> resource_{std::make_unique<memory::lifo_paged_memory_resource>(&global::page_pool())};
};

template <kind ...Kinds>
basic_record_writer create_writer() {
    return basic_record_writer{jogasaki::mock::create_meta<Kinds...>()};
}

template <kind ...Kinds>
std::shared_ptr<basic_record_writer> create_writer_shared() {
    return std::make_shared<basic_record_writer>(jogasaki::mock::create_meta<Kinds...>());
}

template <kind ...Kinds>
std::shared_ptr<basic_record_writer> create_writer_shared(
    std::size_t capacity,
    basic_record_writer::memory_resource_type* resource) {
    return std::make_shared<basic_record_writer>(
        jogasaki::mock::create_meta<Kinds...>(),
        capacity,
        resource
    );
}
}

