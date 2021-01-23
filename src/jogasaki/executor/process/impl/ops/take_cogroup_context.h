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

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/data/iterable_record_store.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/meta/group_meta.h>
#include <jogasaki/executor/group_reader.h>
#include <jogasaki/executor/compare_info.h>
#include <jogasaki/executor/comparator.h>
#include "context_base.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::maybe_shared_ptr;

namespace details {

using checkpoint = memory::lifo_paged_memory_resource::checkpoint;

/**
 * @brief responsible for reading from reader and filling the record store
 */
class cache_align group_input {
public:
    using iterator = data::iterable_record_store::iterator;

    group_input(
        executor::group_reader& reader,
        std::unique_ptr<data::iterable_record_store> store,
        memory::lifo_paged_memory_resource* resource,
        memory::lifo_paged_memory_resource* varlen_resource,
        maybe_shared_ptr<meta::group_meta> meta
    );

    [[nodiscard]] accessor::record_ref current_key() const noexcept;

    [[nodiscard]] accessor::record_ref next_key() const noexcept;

    [[nodiscard]] maybe_shared_ptr<meta::group_meta> const& meta();

    [[nodiscard]] bool eof() const noexcept;

    [[nodiscard]] bool filled() const noexcept;

    /**
     * @return true if key has been read
     * @return false if key has not been read, or reader reached eof
     */
    [[nodiscard]] bool next_key_read() const noexcept;

    [[nodiscard]] iterator begin();

    [[nodiscard]] iterator end();

    [[nodiscard]] bool read_next_key();

    /**
     * @brief fill values
     */
    void fill() noexcept;

    void reset_values();

private:

    executor::group_reader* reader_{};
    std::unique_ptr<data::iterable_record_store> store_{};
    memory::lifo_paged_memory_resource* resource_{};
    memory::lifo_paged_memory_resource* varlen_resource_{};
    memory::lifo_paged_memory_resource::checkpoint resource_last_checkpoint_{};
    memory::lifo_paged_memory_resource::checkpoint varlen_resource_last_checkpoint_{};

    maybe_shared_ptr<meta::group_meta> meta_{};
    std::size_t key_size_ = 0;
    data::small_record_store current_key_; // shallow copy of key (varlen body is held by reader)
    data::small_record_store next_key_;
    bool reader_eof_{false};
    bool values_filled_{false};
    bool next_key_read_{false};
};

/**
 * @brief group input comparator
 * @details comparator to compare group_input with its current key value
 * like std::greater, this comparator returns true when x > y, where x and y are 1st and 2nd args.
 * This is intended to be used with std::priority_queue, which positions the greatest at the top.
 */
class group_input_comparator {
public:
    using input_index = std::size_t;

    /**
     * @brief create undefined object
     */
    group_input_comparator() = default;

    /**
     * @brief construct new object
     * @attention key_meta is kept and used by the comparator. The caller must ensure it outlives this object.
     */
    group_input_comparator(
        std::vector<group_input>* inputs
    );

    [[nodiscard]] bool operator()(input_index const& x, input_index const& y);

private:
    std::vector<group_input>* inputs_{};
};

} // namespace details

/**
 * @brief take_group context
 */
class take_cogroup_context : public context_base {
public:
    friend class take_cogroup;
    using input_index = std::size_t;
    using queue_type = std::priority_queue<input_index, std::vector<input_index>, details::group_input_comparator>;

    /**
     * @brief create empty object
     */
    take_cogroup_context() = default;

    /**
     * @brief create new object
     */
    take_cogroup_context(
        class abstract::task_context* ctx,
        block_scope& variables,
        memory_resource* resource,
        memory_resource* varlen_resource
    );

    [[nodiscard]] operator_kind kind() const noexcept override;

    void release() override;

private:
    std::vector<executor::group_reader*> readers_{};
    std::vector<details::group_input> inputs_{};
    queue_type queue_{};
};

}


