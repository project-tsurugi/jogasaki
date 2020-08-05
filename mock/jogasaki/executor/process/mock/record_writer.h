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

#include <jogasaki/mock/basic_record.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/executor/record_writer.h>
#include <jogasaki/utils/copy_field_data.h>

namespace jogasaki::executor::process::mock {

using kind = meta::field_type_kind;

template <class Record>
class basic_record_writer : public executor::record_writer {
public:

    using record_type = Record;
    using records_type = std::vector<record_type>;

    static constexpr std::size_t npos = static_cast<std::size_t>(-1);

    /**
     * @brief create default instance - written records are stored internally as they are.
     */
    basic_record_writer() = default;

    /**
     * @brief create new instance considering field metadata and its mapping
     * @param meta metadata of the record_ref passed to write()
     * @param map field mapping represented by the pair {source index, target index} where source is the input record, and target is the stored record
     */
    explicit basic_record_writer(std::shared_ptr<meta::record_meta> meta, std::unordered_map<std::size_t, std::size_t> map = {}) :
        meta_(std::move(meta)),
        map_(std::move(map))
    {
        assert(map.empty() || map.size() == meta->field_count());
    }

    explicit basic_record_writer(std::size_t capacity, std::shared_ptr<meta::record_meta> meta ={}, std::unordered_map<std::size_t, std::size_t> map = {}) :
        meta_(std::move(meta)),
        map_(std::move(map)),
        capacity_(capacity)
    {
        assert(map.empty() || map.size() == meta->field_count());
        records_.reserve(capacity);
    }

    /**
     * @brief write record and store internal storage as basic_record.
     * The record_meta, if passed to constructor, is used to convert the offset between input record ref and basic_record::record_meata().
     * Only offsets are converted, nothing done for field ordering.
     */
    bool write(accessor::record_ref rec) override {
        record_type r{};
        if (meta_) {
            for(std::size_t i = 0; i < meta_->field_count(); ++i) {
                auto j = map_.empty() ? i : map_.at(i);
                utils::copy_field(meta_->at(i), r.ref(), r.record_meta()->value_offset(j), rec, meta_->value_offset(i));
            }
        } else {
            r = record_type{rec};
        }
        if (capacity_ == npos || records_.size() < capacity_) {
            records_.emplace_back(r);
        } else {
            records_[pos_ % capacity_] = r;
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
    std::shared_ptr<meta::record_meta> meta_{};
    records_type records_{};
    std::unordered_map<std::size_t, std::size_t> map_{};
    bool released_{false};
    bool acquired_{false};
    std::size_t capacity_{npos};
    std::size_t pos_{};
    std::size_t write_count_{};
};

using record_writer = basic_record_writer<jogasaki::mock::basic_record<kind::int8, kind::float8>>;

template <class Record>
[[nodiscard]] basic_record_writer<Record>* unwrap(executor::record_writer* writer) {
    return static_cast<basic_record_writer<Record>*>(writer);
}

[[nodiscard]] inline record_writer* unwrap_record_writer(executor::record_writer* writer) {
    return unwrap<record_writer::record_type>(writer);
}

}

