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
#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/meta/group_meta.h>
#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/executor/group_reader.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/utils/copy_field_data.h>
#include <jogasaki/utils/interference_size.h>

namespace jogasaki::executor::process::mock {

using takatori::util::maybe_shared_ptr;

using kind = meta::field_type_kind;

class basic_group_entry {
public:
    using key_type = jogasaki::mock::basic_record;
    using value_type = jogasaki::mock::basic_record;
    using value_groups = std::vector<value_type>;

    basic_group_entry(key_type key, value_groups values) : key_(std::move(key)), values_(std::move(values)) {}

    [[nodiscard]] key_type const& key() const noexcept {
        return key_;
    }

    [[nodiscard]] value_groups const& values() const noexcept {
        return values_;
    }

private:
    key_type key_{};
    value_groups values_{};
};

/**
 * @brief mock group reader
 * @details this object holds group entries internally and provide them on demand.
 * External metadata is supported, that allows receiver to get the key/value record as they want.
 */
class cache_align basic_group_reader : public executor::group_reader {
public:
    using group_type = basic_group_entry;
    using groups_type = std::vector<group_type>;

    /**
     * @brief create new object
     * @param groups the group entries the mock object output
     * @param meta the metadata of the records in the internal store
     * @param external_meta the metadata of the output records, when null, output records are in the form defined
     * by the internal metadata (meta parameter above).
     */
    basic_group_reader(
        groups_type groups,
        maybe_shared_ptr<meta::group_meta> meta,
        maybe_shared_ptr<meta::group_meta> external_meta = {}
    ) :
        groups_(std::move(groups)),
        meta_(std::move(meta)),
        external_meta_(std::move(external_meta)),
        key_store_(external_meta_ ? std::make_shared<data::small_record_store>(external_meta_->key_shared()) : nullptr),
        value_store_(external_meta_ ? std::make_shared<data::small_record_store>(external_meta_->value_shared()) : nullptr)
    {}

    bool next_group() override {
        if (!initialized_) {
            current_group_ = groups_.begin();
            initialized_ = true;
        } else {
            ++current_group_;
        }
        on_member_ = false;
        current_member_ = {};
        return current_group_ != groups_.end();
    }

    [[nodiscard]] accessor::record_ref get_group() const override {
        if (external_meta_) {
            auto& r = *current_group_;
            auto rec = key_store_->ref();
            auto& m = external_meta_->key();
            for(std::size_t i = 0; i < m.field_count(); ++i) {
                utils::copy_nullable_field(m.at(i), rec, m.value_offset(i), m.nullity_offset(i), r.key().ref(), r.key().record_meta()->value_offset(i), r.key().record_meta()->nullity_offset(i));
            }
            return rec;
        }
        return current_group_->key().ref();
    }

    bool next_member() override {
        if (!on_member_) {
            current_member_ = const_cast<typename group_type::value_groups&>(current_group_->values()).begin(); //FIXME
            on_member_ = true;
        } else {
            ++current_member_;
        }
        return current_member_ != current_group_->values().end();
    }

    [[nodiscard]] accessor::record_ref get_member() const override {
        if (external_meta_) {
            auto& r = *current_member_;
            auto rec = value_store_->ref();
            auto& m = external_meta_->value();
            for(std::size_t i = 0; i < m.field_count(); ++i) {
                utils::copy_nullable_field(m.at(i), rec, m.value_offset(i), m.nullity_offset(i), r.ref(), r.record_meta()->value_offset(i), r.record_meta()->nullity_offset(i));
            }
            return rec;
        }
        return current_member_->ref();
    }

    void release() override {
        groups_.clear();
        released_ = true;
        initialized_ = false;
    }

    void acquire() {
        acquired_ = true;
    }

    ~basic_group_reader() override = default;
    basic_group_reader(basic_group_reader const& other) = default;
    basic_group_reader& operator=(basic_group_reader const& other) = default;
    basic_group_reader(basic_group_reader&& other) noexcept = default;
    basic_group_reader& operator=(basic_group_reader&& other) noexcept = default;

    [[nodiscard]] bool is_released() const noexcept {
        return released_;
    }
    [[nodiscard]] bool is_acquired() const noexcept {
        return acquired_;
    }
private:
    groups_type groups_{};
    maybe_shared_ptr<meta::group_meta> meta_{};
    maybe_shared_ptr<meta::group_meta> external_meta_{};
    std::shared_ptr<data::small_record_store> key_store_{};
    std::shared_ptr<data::small_record_store> value_store_{};
    typename groups_type::iterator current_group_{};
    typename group_type::value_groups::iterator current_member_{};
    bool initialized_{false};
    bool released_{false};
    bool acquired_{false};
    bool on_member_{false};
};

}

