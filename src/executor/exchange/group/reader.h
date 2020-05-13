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

#include <queue>

#include <glog/logging.h>

#include "executor/group_reader.h"
#include <executor/exchange/group/input_partition.h>

namespace jogasaki::executor::exchange::group {

using iterator = input_partition::table_iterator;

struct iterator_pair {
    iterator_pair(iterator x, iterator y) : first(x), second(y) {}
    iterator first;
    iterator second;
};

static_assert(std::is_trivially_copyable_v<iterator>);
static_assert(std::is_trivially_copyable_v<iterator_pair>);

/**
 * @brief iterator pair comparator
 * @details like std::greater, this comparator returns true when x > y, where x and y are 1st and 2nd args.
 * This is intended to be used with std::priority_queue, which positions the greatest at the top.
 */
class iterator_pair_comparator {
public:
    explicit iterator_pair_comparator(std::shared_ptr<shuffle_info> info) :
            info_(std::move(info)),
            record_size_(info_->record_meta()->record_size()),
            key_comparator_(info_->key_meta()) {}

    bool operator()(iterator_pair const& x, iterator_pair const& y) {
        auto& it_x = x.first;
        auto& it_y = y.first;
        auto key_x = info_->extract_key(accessor::record_ref(*it_x, record_size_));
        auto key_y = info_->extract_key(accessor::record_ref(*it_y, record_size_));
        return key_comparator_(key_x, key_y) > 0;
    }

private:
    std::shared_ptr<shuffle_info> info_{};
    std::size_t record_size_{};
    comparator key_comparator_{};
};

enum class reader_state {
    init,
    before_member,
    on_member,
    after_group,
    eof,
};

/**
 * @brief reader for grouped records
 * @attention readers for shuffle should be acquired after transfer completed
 */
class reader : public group_reader {
public:
    ~reader() override = default;
    reader(reader const& other) = delete;
    reader& operator=(reader const& other) = delete;
    reader(reader&& other) noexcept = delete;
    reader& operator=(reader&& other) noexcept = delete;

    reader(std::shared_ptr<shuffle_info> info, std::vector<std::unique_ptr<input_partition>>& partitions) :
            partitions_(partitions),
            info_(std::move(info)),
            queue_(iterator_pair_comparator(info_)),
            record_size_(info_->record_meta()->record_size()),
            buf_(std::make_unique<char[]>(record_size_)), //NOLINT
            key_comparator_(info_->key_meta()) {
        for(auto& p : partitions_) {
            if (!p) continue;
            for(auto& t : *p) {
                if (t.begin() != t.end()) {
                    queue_.emplace(t.begin(), t.end());
                }
            }
        }
        VLOG(1) << "reader initialized to merge " << queue_.size() << " pointer tables";
    }

    inline void read_and_pop(iterator it, iterator end) { //NOLINT
        queue_.pop();
        memcpy(buf_.get(), *it, record_size_);
        if (++it != end) {
            queue_.emplace(it, end);
        }
    }

    bool next_group() override {
        if (state_ == reader_state::init || state_ == reader_state::after_group) {
            if (queue_.empty()) {
                state_ = reader_state::eof;
                return false;
            }
            auto it = queue_.top().first;
            auto end = queue_.top().second;
            read_and_pop(it, end);
            state_ = reader_state::before_member;
            return true;
        }
        std::abort();
    }

    [[nodiscard]] accessor::record_ref get_group() const override {
        if (state_ == reader_state::before_member || state_ == reader_state::on_member) {
            return info_->extract_key(accessor::record_ref(buf_.get(), record_size_));
        }
        std::abort();
    }

    bool next_member() override {
        if (state_ == reader_state::before_member) {
            state_ = reader_state::on_member;
            return true;
        }
        if(state_ == reader_state::on_member) {
            if (queue_.empty()) {
                state_ = reader_state::after_group;
                return false;
            }
            auto it = queue_.top().first;
            auto end = queue_.top().second;
            if (key_comparator_(
                    info_->extract_key(accessor::record_ref(buf_.get(), record_size_)),
                    info_->extract_key(accessor::record_ref(*it, record_size_))) == 0) {
                read_and_pop(it, end);
                return true;
            }
            state_ = reader_state::after_group;
            return false;
        }
        std::abort();
    }

    [[nodiscard]] accessor::record_ref get_member() const override {
        if (state_ == reader_state::on_member) {
            return info_->extract_value(accessor::record_ref(buf_.get(), record_size_));
        }
        std::abort();
    }

    void release() override {
        // TODO when multiple readers exist for a source, wait for all readers to complete
        partitions_.clear();
    }

private:
    std::vector<std::unique_ptr<input_partition>>& partitions_;
    std::shared_ptr<shuffle_info> info_{};
    std::priority_queue<iterator_pair, std::vector<iterator_pair>, iterator_pair_comparator> queue_;
    std::size_t record_size_{};
    std::unique_ptr<char[]> buf_{}; //NOLINT
    reader_state state_{reader_state::init};
    comparator key_comparator_{};
};

}
