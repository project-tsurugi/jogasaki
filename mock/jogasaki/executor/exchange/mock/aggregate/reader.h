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

#include <jogasaki/executor/io/group_reader.h>
#include <jogasaki/executor/exchange/mock/aggregate/input_partition.h>
#include <jogasaki/utils/interference_size.h>
#include "shuffle_info.h"

namespace jogasaki::executor::exchange::mock::aggregate {

/**
 * @brief reader for aggregate exchange
 */
class cache_align reader : public io::group_reader {
public:
    using iterable_tables = std::vector<input_partition::iterable_hash_table>;
    using aggregator_type = shuffle_info::aggregator_type;
    ~reader() override = default;
    reader(reader const& other) = delete;
    reader& operator=(reader const& other) = delete;
    reader(reader&& other) noexcept = delete;
    reader& operator=(reader&& other) noexcept = delete;

    reader(std::shared_ptr<shuffle_info> info,
        std::vector<std::unique_ptr<input_partition>>& partitions,
        aggregator_type const& aggregator
        );

    [[nodiscard]] bool next_group() override;

    [[nodiscard]] accessor::record_ref get_group() const override;

    [[nodiscard]] bool next_member() override;

    [[nodiscard]] accessor::record_ref get_member() const override;

    void release() override;

private:
    std::vector<std::unique_ptr<input_partition>>& partitions_;
    std::shared_ptr<shuffle_info> info_{};
    aggregator_type const& aggregator_;
    std::size_t key_size_{};
    std::size_t value_size_{};
    iterable_tables tables_{};
    iterable_tables::iterator iterated_table_{};
    bool on_member_{false};
};

}
