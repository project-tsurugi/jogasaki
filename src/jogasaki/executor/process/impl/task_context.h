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

#include <takatori/util/downcast.h>

#include <jogasaki/executor/exchange/aggregate/flow.h>
#include <jogasaki/executor/exchange/forward/flow.h>
#include <jogasaki/executor/exchange/group/flow.h>
#include <jogasaki/executor/exchange/step.h>
#include <jogasaki/executor/io/reader_container.h>
#include <jogasaki/executor/io/record_channel.h>
#include <jogasaki/executor/io/record_writer.h>
#include <jogasaki/executor/process/abstract/scan_info.h>
#include <jogasaki/executor/process/abstract/task_context.h>
#include <jogasaki/executor/process/abstract/work_context.h>
#include <jogasaki/executor/process/impl/ops/emit.h>
#include <jogasaki/executor/process/impl/scan_info.h>
#include <jogasaki/executor/process/io_exchange_map.h>
#include <jogasaki/request_context.h>

namespace jogasaki::executor::process::impl {

using takatori::util::unsafe_downcast;

/**
 * @brief task context implementation for production
 */
class task_context : public abstract::task_context {
public:
    using partition_index = std::size_t;

    /**
     * @brief create new empty instance
     */
    task_context() = default;

    explicit task_context(partition_index partition);

    /**
     * @brief create new object
     * @param partition the index of partition assigned to this object
     * @param io_exchange_map mapping from input/output indices to exchanges
     * @param scan_info the scan information, nullptr if the task doesn't contain scan
     * @param channel the record channel to write the result data
     */
    task_context(
        request_context& rctx,
        partition_index partition,
        io_exchange_map const& io_exchange_map,
        std::shared_ptr<impl::scan_info> scan_info,
        io::record_channel* channel
    );

    io::reader_container reader(reader_index idx) override;

    io::record_writer* downstream_writer(writer_index idx) override;

    io::record_writer* external_writer() override;

    class abstract::scan_info const* scan_info() override;

    [[nodiscard]] std::size_t partition() const noexcept;

    [[nodiscard]] io::record_channel* channel() const noexcept;

    void deactivate_writer(writer_index idx) override;

private:
    request_context* request_context_{};
    std::size_t partition_{};
    io_exchange_map const* io_exchange_map_{};
    std::shared_ptr<impl::scan_info> scan_info_{};
    io::record_channel* channel_{};
    std::shared_ptr<io::record_writer> external_writer_{};
};

}


