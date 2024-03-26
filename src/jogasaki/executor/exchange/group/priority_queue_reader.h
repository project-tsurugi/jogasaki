/*
 * Copyright 2018-2023 Project Tsurugi.
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

#include <cstddef>
#include <memory>
#include <queue>
#include <vector>
#include <glog/logging.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/executor/comparator.h>
#include <jogasaki/executor/exchange/group/group_info.h>
#include <jogasaki/executor/exchange/group/input_partition.h>
#include <jogasaki/executor/io/group_reader.h>
#include <jogasaki/utils/interference_size.h>
#include <jogasaki/utils/iterator_pair.h>

namespace jogasaki::executor::exchange::group {

namespace impl {

using iterator = input_partition::table_iterator;

using iterator_pair = utils::iterator_pair<iterator>;

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
    explicit iterator_pair_comparator(group_info const* info);

    [[nodiscard]] bool operator()(iterator_pair const& x, iterator_pair const& y);

private:
    group_info const* info_{};
    std::size_t record_size_{};
    comparator key_comparator_{};
};

} // namespace impl

/**
 * @brief priority queue based reader for grouped records
 * @details pregrouped pointer tables are k-way merged using priority queue
 * @attention readers for shuffle should be acquired after transfer completed
 */
class cache_align priority_queue_reader : public io::group_reader {
public:
    ~priority_queue_reader() override = default;
    priority_queue_reader(priority_queue_reader const& other) = delete;
    priority_queue_reader& operator=(priority_queue_reader const& other) = delete;
    priority_queue_reader(priority_queue_reader&& other) noexcept = delete;
    priority_queue_reader& operator=(priority_queue_reader&& other) noexcept = delete;

    priority_queue_reader(std::shared_ptr<group_info> info, std::vector<std::unique_ptr<input_partition>>& partitions);

    [[nodiscard]] bool next_group() override;

    [[nodiscard]] accessor::record_ref get_group() const override;

    [[nodiscard]] bool next_member() override;

    [[nodiscard]] accessor::record_ref get_member() const override;

    void release() override;

private:
    std::vector<std::unique_ptr<input_partition>>& partitions_;
    std::shared_ptr<group_info> info_{};
    std::priority_queue<impl::iterator_pair, std::vector<impl::iterator_pair>, impl::iterator_pair_comparator> queue_;
    std::size_t record_size_{};

    // buffer for shallow copy. Not associated with varlen memory resource because this is temporary for comparison
    // and the ownership is held on original in the input partition
    data::small_record_store buf_;

    impl::reader_state state_{impl::reader_state::init};
    comparator key_comparator_{};

    std::size_t record_count_per_group_{};

    void pop_queue(bool read);
    void discard_remaining_members_in_group();
};

}
