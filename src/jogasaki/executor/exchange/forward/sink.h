/*
 * Copyright 2018-2024 Project Tsurugi.
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

#include <memory>

#include <jogasaki/constants.h>
#include <jogasaki/executor/exchange/sink.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/io/record_writer.h>
#include <jogasaki/memory/monotonic_paged_memory_resource.h>

#include "source.h"
#include "input_partition.h"

namespace jogasaki::executor::exchange::forward {

class writer;

class sink : public exchange::sink {
public:

    sink() = default;
    ~sink() override = default;
    sink(sink const& other) = delete;
    sink& operator=(sink const& other) = delete;
    sink(sink&& other) noexcept = delete;
    sink& operator=(sink&& other) noexcept = delete;
    sink(
        std::shared_ptr<forward_info> info,
        request_context* context,
        std::shared_ptr<std::atomic_size_t> write_count,
        std::shared_ptr<input_partition> partition
    );

    [[nodiscard]] io::record_writer& acquire_writer() override;

    void release_writer(io::record_writer& writer);

    [[nodiscard]] std::shared_ptr<input_partition> const& partition();

    [[nodiscard]] request_context* context() const noexcept;

    void deactivate() override;

private:
    std::shared_ptr<forward_info> info_{};
    request_context* context_{};
    std::unique_ptr<forward::writer> writer_;
    std::shared_ptr<std::atomic_size_t> write_count_{};
    std::shared_ptr<input_partition> partition_{};
};

}  // namespace jogasaki::executor::exchange::forward
