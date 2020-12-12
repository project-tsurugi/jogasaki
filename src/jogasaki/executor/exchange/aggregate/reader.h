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

#include <jogasaki/data/small_record_store.h>
#include <jogasaki/executor/group_reader.h>
#include <jogasaki/executor/exchange/aggregate/input_partition.h>
#include <jogasaki/utils/interference_size.h>
#include <jogasaki/utils/iterator_pair.h>

namespace jogasaki::executor::exchange::aggregate {

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
    explicit iterator_pair_comparator(aggregate_info const* info) :
        info_(info),
        record_size_(info_->record_meta()->record_size()),
        key_comparator_(info_->mid().group_meta()->key_shared().get()) {}

    [[nodiscard]] bool operator()(iterator_pair const& x, iterator_pair const& y) {
        auto& it_x = x.first;
        auto& it_y = y.first;
        auto key_x = info_->extract_key(accessor::record_ref(*it_x, record_size_));
        auto key_y = info_->extract_key(accessor::record_ref(*it_y, record_size_));
        return key_comparator_(key_x, key_y) > 0;
    }

private:
    aggregate_info const* info_{};
    std::size_t record_size_{};
    comparator key_comparator_{};
};

} // namespace impl

/**
 * @brief reader for aggregate exchange
 */
class cache_align reader : public group_reader {
public:
    using reader_state = impl::reader_state;
    ~reader() override = default;
    reader(reader const& other) = delete;
    reader& operator=(reader const& other) = delete;
    reader(reader&& other) noexcept = delete;
    reader& operator=(reader&& other) noexcept = delete;

    reader(
        std::shared_ptr<aggregate_info> info,
        std::vector<std::unique_ptr<input_partition>>& partitions
    );

    [[nodiscard]] bool next_group() override;

    [[nodiscard]] accessor::record_ref get_group() const override;

    [[nodiscard]] bool next_member() override;

    [[nodiscard]] accessor::record_ref get_member() const override;

    void release() override;

private:
    std::vector<std::unique_ptr<input_partition>>& partitions_;
    std::shared_ptr<aggregate_info> info_{};
    std::priority_queue<impl::iterator_pair, std::vector<impl::iterator_pair>, impl::iterator_pair_comparator> queue_;
    std::size_t key_size_{};
    std::size_t mid_value_size_{};
    data::small_record_store key_buf_;
    data::small_record_store mid_value_buf_;
    data::small_record_store post_value_buf_;
    impl::reader_state state_{impl::reader_state::init};
    comparator key_comparator_{};
    std::size_t pointer_field_offset_{};
    std::vector<std::vector<field_locator>> args_{};
    bool internal_on_member_{};

    bool internal_next_member();
    [[nodiscard]] accessor::record_ref internal_get_member() const;

    inline void read_and_pop(impl::iterator it, impl::iterator end);

    inline void* value_pointer(accessor::record_ref ref) const;
};

}
