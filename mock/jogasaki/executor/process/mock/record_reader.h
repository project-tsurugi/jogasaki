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

#include <takatori/util/sequence_view.h>

#include <jogasaki/accessor/record_ref.h>

#include <jogasaki/executor/record_reader.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/utils/copy_field_data.h>

namespace jogasaki::executor::process::mock {

using kind = meta::field_type_kind;

template <class Record>
class basic_record_reader : public executor::record_reader {
public:
    using record_type = Record;
    using records_type = std::vector<record_type>;

    /**
     * @brief create default instance - read records are output as they are.
     */
    basic_record_reader() = default;

    /**
     * @brief create new instance considering field metadata and its mapping
     * @param records the source records stored internally in this reader
     * @param meta metadata of the record_ref output by get_record()
     * @param map field mapping represented by the pair {source index, target index} where source is the stored record, and target is the output record by get_record()
     */
    explicit basic_record_reader(records_type records, std::shared_ptr<meta::record_meta> meta = {}, std::unordered_map<std::size_t, std::size_t> map = {}) noexcept :
        records_(std::move(records)),
        meta_(std::move(meta)),
        store_(meta_ ? std::make_shared<data::small_record_store>(meta_) : nullptr),
        map_(std::move(map))
    {
        assert(map.empty() || map.size() == meta->field_count());
    }

    [[nodiscard]] bool available() const override {
        return it_ != records_.end() && it_+1 != records_.end();
    }

    bool next_record() override {
        if (!initialized_) {
            it_ = records_.begin();
            initialized_ = true;
        } else {
            if (it_ == records_.end()) {
                return false;
            }
            ++it_;
        }
        ++num_calls_next_record_;
        return it_ != records_.end();
    }

    [[nodiscard]] accessor::record_ref get_record() const override {
        auto rec = it_->ref();
        if (meta_) {
            auto r = *it_;
            rec = store_->ref();
            for(std::size_t i = 0; i < meta_->field_count(); ++i) {
                auto j = map_.empty() ? i : map_.at(i);
                utils::copy_field(meta_->at(j), rec, meta_->value_offset(j), r.ref(), r.record_meta()->value_offset(i));
            }
        }
        return rec;
    }

    void release() override {
        records_.clear();
        released_ = true;
    }

    void acquire() {
        acquired_ = true;
    }

    std::shared_ptr<meta::record_meta> const& meta() {
        static record_type rec{};
        if (meta_) {
            return meta_;
        }
        return rec.record_meta();
    }

    [[nodiscard]] bool is_released() const noexcept {
        return released_;
    }

    [[nodiscard]] bool is_acquired() const noexcept {
        return acquired_;
    }

    [[nodiscard]] std::size_t num_calls_next_record() const noexcept {
        return num_calls_next_record_;
    }
private:
    records_type records_{};
    std::shared_ptr<meta::record_meta> meta_{};
    std::shared_ptr<data::small_record_store> store_{};
    std::unordered_map<std::size_t, std::size_t> map_{};
    bool initialized_{false};
    bool released_{false};
    bool acquired_{false};
    std::size_t num_calls_next_record_{};
    typename records_type::iterator it_{};
};

using record_reader = basic_record_reader<jogasaki::mock::basic_record<kind::int8, kind::float8>>;

template <class Record>
basic_record_reader<Record>* unwrap(executor::record_reader* reader) {
    return static_cast<basic_record_reader<Record>*>(reader);
}

inline record_reader* unwrap_record_reader(executor::record_reader* reader) {
    return unwrap<record_reader::record_type>(reader);
}

}

