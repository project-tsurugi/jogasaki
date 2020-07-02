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

#include <jogasaki/request_context.h>
#include <jogasaki/executor/exchange/source.h>
#include <jogasaki/executor/exchange/aggregate/shuffle_info.h>
#include <jogasaki/executor/exchange/aggregate/input_partition.h>
#include <jogasaki/executor/group_reader.h>

namespace jogasaki::executor::exchange::aggregate {

class source : public exchange::source {
public:
    source();
    ~source() override;
    source(source const& other) = delete;
    source& operator=(source const& other) = delete;
    source(source&& other) noexcept = delete;
    source& operator=(source&& other) noexcept = delete;
    explicit source(
            std::shared_ptr<shuffle_info> info,
            request_context* context
            );
    void receive(std::unique_ptr<input_partition> in);

    reader_container acquire_reader() override;

private:
    std::vector<std::unique_ptr<group_reader>> readers_;
    std::shared_ptr<shuffle_info> info_{};
    request_context* context_{};
    std::vector<std::unique_ptr<input_partition>> partitions_{};
};

}
