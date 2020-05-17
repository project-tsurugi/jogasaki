
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

#include <utils/watch.h>

#include "executor/group_reader.h"
#include <executor/exchange/group/input_partition.h>

namespace jogasaki::executor::exchange::group {

/**
 * @brief reader non-priority queue implementation
 * @attention readers for shuffle should be acquired after transfer completed
 */
class sorted_vector_reader : public group_reader {
public:
    enum class reader_state {
        init,
        before_member,
        on_member,
        after_group,
        eof,
    };

    using iterator = input_partition::table_iterator;

    using pointer = pointer_table::pointer;

    static_assert(std::is_trivially_copyable_v<iterator>);

    ~sorted_vector_reader() override = default;
    sorted_vector_reader(sorted_vector_reader const& other) = delete;
    sorted_vector_reader& operator=(sorted_vector_reader const& other) = delete;
    sorted_vector_reader(sorted_vector_reader&& other) noexcept = delete;
    sorted_vector_reader& operator=(sorted_vector_reader&& other) noexcept = delete;

    sorted_vector_reader(std::shared_ptr<shuffle_info> info, std::vector<std::unique_ptr<input_partition>>& partitions);

    bool next_group() override;

    [[nodiscard]] accessor::record_ref get_group() const override;

    bool next_member() override;

    [[nodiscard]] accessor::record_ref get_member() const override;

    void release() override;

private:
    std::vector<std::unique_ptr<input_partition>>& partitions_;
    std::shared_ptr<shuffle_info> info_{};
    std::size_t record_size_{};
    std::unique_ptr<char[]> buf_{}; //NOLINT
    reader_state state_{reader_state::init};
    comparator key_comparator_{};
    std::vector<pointer> aggregated_pointer_table_{};
    bool aggregated_pointer_table_initialized = false;
    std::vector<pointer>::iterator current_{};

    inline void read_and_pop();
    void init_aggregated_table();
};

}
