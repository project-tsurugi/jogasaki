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

#include <jogasaki/meta/record_meta.h>
#include <jogasaki/executor/record_writer.h>
#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/utils/copy_field_data.h>
#include <jogasaki/utils/interference_size.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>

namespace jogasaki::executor::process::mock {

using kind = meta::field_type_kind;

template <class Record>
class cache_align basic_external_writer : public executor::record_writer {
public:
    using record_type = Record;
    using records_type = std::vector<record_type>;

    /**
     * @brief create default instance - written records are stored internally as they are.
     */
    basic_external_writer() = default;

    /**
     * @brief create new instance considering field metadata and its mapping
     * @param meta metadata of the record_ref passed to write()
     * @param map field mapping represented by the pair {source index, target index} where source is the input record, and target is the stored record
     */
    explicit basic_external_writer(maybe_shared_ptr<meta::record_meta> meta, std::unordered_map<std::size_t, std::size_t> map = {}) :
        meta_(std::move(meta)),
        map_(std::move(map))
    {
        assert(map.empty() || map.size() == meta->field_count());
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
                utils::copy_field(meta_->at(i), r.ref(), r.record_meta()->value_offset(j), rec, meta_->value_offset(i), resource_.get());
            }
        } else {
            r = record_type{rec, resource_.get()};
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

    void acquire() {
        acquired_ = true;
    }

    [[nodiscard]] std::size_t size() const noexcept {
        return records_.size();
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
    std::unordered_map<std::size_t, std::size_t> map_{};
    bool released_{false};
    bool acquired_{false};
    std::unique_ptr<memory::paged_memory_resource> resource_{std::make_unique<memory::lifo_paged_memory_resource>(&global::page_pool())};
};

using external_writer = basic_external_writer<jogasaki::mock::basic_record>;

}

