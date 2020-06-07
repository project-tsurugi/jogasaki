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

#include <jogasaki/record.h>

namespace jogasaki::executor::process::mock {

class group_entry {
public:
    using value_type = std::vector<double>;
    group_entry(
            std::int64_t key,
            std::vector<double> values
    ) : key_(key), values_(std::move(values)) {}

    std::int64_t key_{};
    value_type values_{};
};

class group_reader : public executor::group_reader {
public:
    using group_type = std::vector<group_entry>;

    group_reader(
            group_type groups
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
        return accessor::record_ref(&current_group_->key_, sizeof(std::int64_t));
    }

    bool next_member() override {
        if (!on_member_) {
            current_member_ = current_group_->values_.begin();
            on_member_ = true;
        } else {
            ++current_member_;
        }
        return current_member_ != current_group_->values_.end();
    }

    [[nodiscard]] accessor::record_ref get_member() const override {
        return accessor::record_ref(&*current_member_, sizeof(double));
    }

    void release() override {
        groups_.clear();
        released_ = true;
        initialized_ = false;
    }

    ~group_reader() override = default;
    group_reader(group_reader const& other) = default;
    group_reader& operator=(group_reader const& other) = default;
    group_reader(group_reader&& other) noexcept = default;
    group_reader& operator=(group_reader&& other) noexcept = default;

    group_type groups_{};
    group_type::iterator current_group_{};
    group_entry::value_type::iterator current_member_{};
    bool initialized_{false};
    bool released_{false};
    bool on_member_{false};
};

}

