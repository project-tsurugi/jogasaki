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

    basic_record_writer() = default;

    /**
     * @param meta metadata of the record_ref passed to write()
     */
    explicit basic_record_writer(std::shared_ptr<meta::record_meta> meta) : meta_(std::move(meta)) {}


    /**
     * @brief write record and store internal storage as basic_record.
     * The record_meta, if passed to constructor, is used to convert the offset between input record ref and basic_record::record_meata().
     * Only offsets are converted, nothing done for field ordering.
     */
    bool write(accessor::record_ref rec) override {
        record_type r{};
        if (meta_) {
            for(std::size_t i = 0; i < meta_->field_count(); ++i) {
                utils::copy_field(meta_->at(i), r.ref(), r.record_meta()->value_offset(i), rec, meta_->value_offset(i));
            }
        } else {
            r = record_type{rec};
        }
        records_.emplace_back(r);
        return false;
    }

    void flush() override {
        // no-op
    }

    void release() override {
        released_ = true;
    }

    [[nodiscard]] std::size_t size() const noexcept {
        return records_.size();
    }

    records_type const& records() const noexcept {
        return records_;
    }

private:
    std::shared_ptr<meta::record_meta> meta_{};
    records_type records_{};
    bool released_{false};
};

using record_writer = basic_record_writer<basic_record<kind::int8, kind::float8>>;

template <class Record>
basic_record_writer<Record>* unwrap(executor::record_writer* writer) {
    return static_cast<basic_record_writer<Record>*>(writer);
}

inline record_writer* unwrap_record_writer(executor::record_writer* writer) {
    return unwrap<record_writer::record_type>(writer);
}

}

