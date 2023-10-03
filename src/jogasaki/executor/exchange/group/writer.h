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

#include <jogasaki/constants.h>
#include <jogasaki/executor/io/record_writer.h>
#include "input_partition.h"
#include "group_info.h"
#include "sink.h"

namespace jogasaki::executor::exchange::group {

class cache_align writer : public io::record_writer {
public:
    ~writer() override = default;
    writer(writer const& other) = delete;
    writer& operator=(writer const& other) = delete;
    writer(writer&& other) noexcept = delete;
    writer& operator=(writer&& other) noexcept = delete;

    writer(
        std::size_t downstream_partitions,
        std::shared_ptr<group_info> info,
        std::vector<std::unique_ptr<input_partition>>& partitions,
        group::sink& owner
    );

    bool write(accessor::record_ref rec) override;

    void flush() override;

    void release() override;

private:
    std::size_t downstream_partitions_{default_partitions};
    std::vector<std::unique_ptr<input_partition>>& partitions_;
    std::shared_ptr<group_info> info_{};
    partitioner partitioner_{};
    sink* owner_{};

    void initialize_lazy(std::size_t partition);
};

}
