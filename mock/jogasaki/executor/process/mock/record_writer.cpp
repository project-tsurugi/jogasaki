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
#include "record_writer.h"

#include <jogasaki/meta/record_meta.h>

namespace jogasaki::executor::process::mock {

basic_record_writer::basic_record_writer(maybe_shared_ptr <meta::record_meta> meta) :
    meta_(std::move(meta))
{}

basic_record_writer::basic_record_writer(
    maybe_shared_ptr <meta::record_meta> meta,
    std::size_t capacity,
    basic_record_writer::memory_resource_type* resource
) :
    meta_(std::move(meta)),
    records_(resource),
    capacity_(capacity)
{
    BOOST_ASSERT(capacity_ > 0);  //NOLINT
    records_.reserve(capacity);
}

bool basic_record_writer::write(accessor::record_ref rec) {
    record_type r{rec, meta_, resource_.get()};
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

void basic_record_writer::flush() {
    // no-op
}

void basic_record_writer::release() {
    released_ = true;
}

void basic_record_writer::acquire() {
    acquired_ = true;
}

std::size_t basic_record_writer::size() const noexcept {
    return std::max(write_count_, records_.size());
}

const basic_record_writer::records_type& basic_record_writer::records() const noexcept {
    return records_;
}

bool basic_record_writer::is_released() const noexcept {
    return released_;
}

bool basic_record_writer::is_acquired() const noexcept {
    return acquired_;
}
}

