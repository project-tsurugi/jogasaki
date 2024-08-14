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

#include <memory>

#include <jogasaki/executor/exchange/forward/input_partition.h>
#include <jogasaki/executor/exchange/source.h>
#include <jogasaki/executor/io/reader_container.h>
#include <jogasaki/executor/io/record_reader.h>

#include "forward_info.h"

namespace jogasaki::executor::exchange::forward {

class reader;

class source : public exchange::source {
public:

    source();
    ~source() override;
    source(source const& other) = delete;
    source& operator=(source const& other) = delete;
    source(source&& other) noexcept = delete;
    source& operator=(source&& other) noexcept = delete;
    explicit source(
        std::shared_ptr<forward_info> info,
        request_context* context,
        std::shared_ptr<input_partition> partion,
        std::shared_ptr<std::atomic_bool> sink_active
    );
    [[nodiscard]] io::reader_container acquire_reader() override;

    [[nodiscard]] std::shared_ptr<input_partition> const& partition();
private:
    std::unique_ptr<io::record_reader> reader_;
    std::shared_ptr<forward_info> info_{};
    request_context* context_{};
    std::shared_ptr<input_partition> partition_{};
    std::shared_ptr<std::atomic_bool> sink_active_{};
};

}  // namespace jogasaki::executor::exchange::forward
