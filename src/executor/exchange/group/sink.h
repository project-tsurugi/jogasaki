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

#include <constants.h>
#include <executor/global.h>
#include <memory/monotonic_paged_memory_resource.h>
#include <executor/record_writer.h>
#include <executor/exchange/sink.h>
#include "input_partition.h"
#include "shuffle_info.h"
#include "source.h"

namespace dc::executor::exchange::group {

class writer;

class sink : public exchange::sink {
public:
    friend class writer;
    sink() = default;
    sink(sink&& other) noexcept = delete;
    sink& operator=(sink&& other) noexcept = delete;
    sink(std::size_t downstream_partitions, std::shared_ptr<shuffle_info> info);

    record_writer& acquire_writer() override;

    void release_writer(record_writer& writer);

    std::vector<std::unique_ptr<input_partition>>& input_partitions();

private:
    std::size_t downstream_partitions_{default_partitions};
    std::vector<std::unique_ptr<input_partition>> partitions_{};
    std::shared_ptr<shuffle_info> info_{};
    partitioner partitioner_{};
    std::unique_ptr<group::writer> writer_;

    void initialize_lazy(std::size_t partition);
};

}
