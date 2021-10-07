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

#include <takatori/util/downcast.h>

#include <jogasaki/executor/reader_container.h>
#include <jogasaki/executor/record_writer.h>
#include <jogasaki/executor/process/abstract/task_context.h>
#include <jogasaki/executor/process/abstract/work_context.h>
#include <jogasaki/executor/process/abstract/scan_info.h>
#include <jogasaki/executor/process/io_exchange_map.h>
#include <jogasaki/executor/process/impl/scan_info.h>
#include <jogasaki/executor/process/impl/ops/emit.h>
#include <jogasaki/executor/exchange/step.h>
#include <jogasaki/executor/exchange/group/flow.h>
#include <jogasaki/executor/exchange/aggregate/flow.h>
#include <jogasaki/executor/exchange/forward/flow.h>

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
     * @param result the store to keep the result data
     */
    task_context(
        partition_index partition,
        io_exchange_map const& io_exchange_map,
        std::shared_ptr<impl::scan_info> scan_info,
        data::iterable_record_store* result,
        api::data_channel* channel
    );

    reader_container reader(reader_index idx) override;

    record_writer* downstream_writer(writer_index idx) override;

    record_writer* external_writer(writer_index idx) override;

    class abstract::scan_info const* scan_info() override;

    [[nodiscard]] std::size_t partition() const noexcept;

    [[nodiscard]] api::data_channel* channel() const noexcept {
        return channel_;
    }
private:
    std::size_t partition_{};
    io_exchange_map const* io_exchange_map_{};
    std::shared_ptr<impl::scan_info> scan_info_{};
    data::iterable_record_store* result_{};
    api::data_channel* channel_{};
    std::vector<std::shared_ptr<record_writer>> external_writers_{};
};

}


