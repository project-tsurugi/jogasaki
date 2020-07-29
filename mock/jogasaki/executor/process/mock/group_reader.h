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

#include <jogasaki/mock/basic_record.h>
#include <jogasaki/executor/group_reader.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/utils/copy_field_data.h>

namespace jogasaki::executor::process::mock {

using kind = meta::field_type_kind;

template <class Key, class Value>
class basic_group_entry {
public:
    using key_type = Key;
    using value_type = Value;
    using value_groups = std::vector<Value>;
    explicit basic_group_entry(Key key, value_groups values) : key_(key), values_(std::move(values)) {}

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

template <class Key, class Value>
class basic_group_reader : public executor::group_reader {
public:
    using group_type = basic_group_entry<Key, Value>;
    using groups_type = std::vector<group_type>;

    explicit basic_group_reader(
        groups_type groups,
        std::shared_ptr<meta::group_meta> meta = {}
    ) :
        groups_(std::move(groups)),
        meta_(std::move(meta)),
        key_store_(meta_ ? std::make_shared<data::small_record_store>(meta_->key_shared()) : nullptr),
        value_store_(meta_ ? std::make_shared<data::small_record_store>(meta_->value_shared()) : nullptr)
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
        if (meta_) {
            auto& r = *current_group_;
            auto rec = key_store_->ref();
            auto& m = meta_->key();
            for(std::size_t i = 0; i < m.field_count(); ++i) {
                utils::copy_field(m.at(i), rec, m.value_offset(i), r.key().ref(), r.key().record_meta()->value_offset(i));
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
        if (meta_) {
            auto& r = *current_member_;
            auto rec = value_store_->ref();
            auto& m = meta_->value();
            for(std::size_t i = 0; i < m.field_count(); ++i) {
                utils::copy_field(m.at(i), rec, m.value_offset(i), r.ref(), r.record_meta()->value_offset(i));
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
    std::shared_ptr<meta::group_meta> meta_{};
    std::shared_ptr<data::small_record_store> key_store_{};
    std::shared_ptr<data::small_record_store> value_store_{};
    typename groups_type::iterator current_group_{};
    typename group_type::value_groups::iterator current_member_{};
    bool initialized_{false};
    bool released_{false};
    bool acquired_{false};
    bool on_member_{false};
};

using group_reader = basic_group_reader<jogasaki::mock::basic_record<kind::int8>, jogasaki::mock::basic_record<kind::float8>>;
}

