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
#include <set>
#include <memory>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/constants.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/meta/group_meta.h>
#include <jogasaki/executor/partitioner.h>
#include <jogasaki/executor/comparator.h>

namespace jogasaki::executor::exchange::group {

using takatori::util::maybe_shared_ptr;

/**
 * @brief information to execute shuffle, used to extract schema and record layout information for key/value parts
 */
class shuffle_info {
public:
    using field_index_type = meta::record_meta::field_index_type;

    /**
     * @brief construct empty object
     */
    shuffle_info() : record_(std::make_shared<meta::record_meta>()), group_(std::make_shared<meta::group_meta>()) {};

    /**
     * @brief construct new object
     * @param record
     * @param key_indices
     * @param partitions
     */
    shuffle_info(maybe_shared_ptr<meta::record_meta> record, std::vector<field_index_type> key_indices) :
            record_(std::move(record)),
            key_indices_(std::move(key_indices)),
            group_(std::make_shared<meta::group_meta>(create_key_meta(), create_value_meta())) {}


    /**
     * @brief extract key part from the input record
     */
    [[nodiscard]] accessor::record_ref extract_key(accessor::record_ref record) const noexcept {
        return accessor::record_ref(record.data(), record_->record_size());
    }

    /**
     * @brief extract value part from the input record
     */
    [[nodiscard]] accessor::record_ref extract_value(accessor::record_ref record) const noexcept {
        return accessor::record_ref(record.data(), record_->record_size());
    }

    /**
     * @brief returns metadata for whole record
     */
    [[nodiscard]] maybe_shared_ptr<meta::record_meta> const& record_meta() const noexcept {
        return record_;
    }

    /**
     * @brief returns metadata for key part
     */
    [[nodiscard]] maybe_shared_ptr<meta::record_meta> const& key_meta() const noexcept {
        return group_->key_shared();
    }

    /**
     * @brief returns metadata for value part
     */
    [[nodiscard]] maybe_shared_ptr<meta::record_meta> const& value_meta() const noexcept {
        return group_->value_shared();
    }

    /**
     * @brief returns metadata for key/value parts at once
     */
    [[nodiscard]] maybe_shared_ptr<meta::group_meta> const& group_meta() const noexcept {
        return group_;
    }

private:
    maybe_shared_ptr<meta::record_meta> record_{};
    std::vector<field_index_type> key_indices_{};
    maybe_shared_ptr<meta::group_meta> group_{};

    [[nodiscard]] std::shared_ptr<meta::record_meta> create_meta(std::vector<std::size_t> const& indices) {
        auto num = indices.size();
        meta::record_meta::fields_type fields{};
        meta::record_meta::nullability_type  nullables(num);
        meta::record_meta::value_offset_table_type value_offset_table{};
        meta::record_meta::nullity_offset_table_type nullity_offset_table{};
        fields.reserve(num);
        value_offset_table.reserve(num);
        nullity_offset_table.reserve(num);
        for(std::size_t i=0; i < num; ++i) {
            auto ind = indices[i];
            fields.emplace_back(record_->at(ind));
            value_offset_table.emplace_back(record_->value_offset(ind));
            nullity_offset_table.emplace_back(record_->nullity_offset(ind));
            if (record_->nullable(ind)) {
                nullables.set(i);
            }
        }
        return std::make_shared<meta::record_meta>(
                std::move(fields),
                std::move(nullables),
                std::move(value_offset_table),
                std::move(nullity_offset_table),
                record_->record_alignment(),
                record_->record_size()
        );

    }

    [[nodiscard]] std::shared_ptr<meta::record_meta> create_key_meta() {
        return create_meta(key_indices_);
    }

    [[nodiscard]] std::shared_ptr<meta::record_meta> create_value_meta() {
        std::size_t num = record_->field_count() - key_indices_.size();
        std::set<std::size_t> indices{key_indices_.begin(), key_indices_.end()};
        std::vector<field_index_type> vec{};
        vec.reserve(num);
        for(std::size_t i=0; i < record_->field_count(); ++i) {
            if (indices.count(i) == 0) {
                vec.emplace_back(i);
            }
        }
        return create_meta(vec);
    }
};

}
