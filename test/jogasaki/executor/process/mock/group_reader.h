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

#include <jogasaki/basic_record.h>

namespace jogasaki::executor::process::mock {

using kind = meta::field_type_kind;

template <class Keys, class Values>
class basic_group_entry {
public:
    using keys_type = Keys;
    using values_type = Values;
    using values_groups = std::vector<Values>;
    explicit basic_group_entry(Keys keys, values_groups values) : keys_(keys), values_(std::move(values)) {}

    [[nodiscard]] accessor::record_ref keys() const noexcept {
        return keys_.ref();
    }

    [[nodiscard]] values_groups const& values() const noexcept {
        return values_;
    }
private:
    keys_type keys_{};
    values_groups values_{};
};

template <class Keys, class Values>
class basic_group_reader : public executor::group_reader {
public:
    using group_type = basic_group_entry<Keys, Values>;
    using groups_type = std::vector<group_type>;

    basic_group_reader(
            groups_type groups
            ) : groups_(std::move(groups)){}

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
        return current_group_->keys();
    }

    bool next_member() override {
        if (!on_member_) {
            current_member_ = const_cast<typename group_type::values_groups&>(current_group_->values()).begin(); //FIXME
            on_member_ = true;
        } else {
            ++current_member_;
        }
        return current_member_ != current_group_->values().end();
    }

    [[nodiscard]] accessor::record_ref get_member() const override {
        return current_member_->ref();
    }

    void release() override {
        groups_.clear();
        released_ = true;
        initialized_ = false;
    }

    ~basic_group_reader() override = default;
    basic_group_reader(basic_group_reader const& other) = default;
    basic_group_reader& operator=(basic_group_reader const& other) = default;
    basic_group_reader(basic_group_reader&& other) noexcept = default;
    basic_group_reader& operator=(basic_group_reader&& other) noexcept = default;

    groups_type groups_{};
    typename groups_type::iterator current_group_{};
    typename group_type::values_groups::iterator current_member_{};
    bool initialized_{false};
    bool released_{false};
    bool on_member_{false};
};

using group_reader = basic_group_reader<testing::basic_record<kind::int8>, testing::basic_record<kind::float8>>;
using group_entry = group_reader::group_type;
}

