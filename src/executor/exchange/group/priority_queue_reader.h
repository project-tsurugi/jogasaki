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
#include <utils/aligned_unique_ptr.h>

namespace jogasaki::executor::exchange::group {

namespace impl {

using iterator = input_partition::table_iterator;

struct iterator_pair {
    iterator_pair(iterator x, iterator y) : first(x), second(y) {}
    iterator first; //NOLINT
    iterator second; //NOLINT
};

static_assert(std::is_trivially_copyable_v<iterator>);
static_assert(std::is_trivially_copyable_v<iterator_pair>);

enum class reader_state {
    init,
    before_member,
    on_member,
    after_group,
    eof,
};

/**
 * @brief iterator pair comparator
 * @details like std::greater, this comparator returns true when x > y, where x and y are 1st and 2nd args.
 * This is intended to be used with std::priority_queue, which positions the greatest at the top.
 */
class iterator_pair_comparator {
public:
    /**
     * @brief construct new object
     * @param info shuffle information
     * @attention info is kept and used by the comparator. The caller must ensure it outlives this object.
     */
    explicit iterator_pair_comparator(shuffle_info const* info) :
            info_(info),
            record_size_(info_->record_meta()->record_size()),
            key_comparator_(info_->key_meta().get()) {}

    bool operator()(iterator_pair const& x, iterator_pair const& y) {
        auto& it_x = x.first;
        auto& it_y = y.first;
        auto key_x = info_->extract_key(accessor::record_ref(*it_x, record_size_));
        auto key_y = info_->extract_key(accessor::record_ref(*it_y, record_size_));
        return key_comparator_(key_x, key_y) > 0;
    }

private:
    shuffle_info const* info_{};
    std::size_t record_size_{};
    comparator key_comparator_{};
};

} // namespace impl

/**
 * @brief priority queue based reader for grouped records
 * @details pregrouped pointer tables are k-way merged using priority queue
 * @attention readers for shuffle should be acquired after transfer completed
 */
class priority_queue_reader : public group_reader {
public:
    ~priority_queue_reader() override = default;
    priority_queue_reader(priority_queue_reader const& other) = delete;
    priority_queue_reader& operator=(priority_queue_reader const& other) = delete;
    priority_queue_reader(priority_queue_reader&& other) noexcept = delete;
    priority_queue_reader& operator=(priority_queue_reader&& other) noexcept = delete;

    priority_queue_reader(std::shared_ptr<shuffle_info> info, std::vector<std::unique_ptr<input_partition>>& partitions);

    bool next_group() override;

    [[nodiscard]] accessor::record_ref get_group() const override;

    bool next_member() override;

    [[nodiscard]] accessor::record_ref get_member() const override;

    void release() override;

private:
    std::vector<std::unique_ptr<input_partition>>& partitions_;
    std::shared_ptr<shuffle_info> info_{};
    std::priority_queue<impl::iterator_pair, std::vector<impl::iterator_pair>, impl::iterator_pair_comparator> queue_;
    std::size_t record_size_{};
    utils::aligned_array<char> buf_;
    impl::reader_state state_{impl::reader_state::init};
    comparator key_comparator_{};

    inline void read_and_pop(impl::iterator it, impl::iterator end);
};

}
