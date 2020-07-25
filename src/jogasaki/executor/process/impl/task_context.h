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

#include <jogasaki/executor/reader_container.h>
#include <jogasaki/executor/record_writer.h>
#include <jogasaki/executor/process/abstract/task_context.h>
#include <jogasaki/executor/process/abstract/work_context.h>
#include <jogasaki/executor/process/abstract/scan_info.h>
#include <jogasaki/executor/process/impl/ops/process_io_map.h>
#include <jogasaki/executor/exchange/step.h>
#include <jogasaki/executor/exchange/group/flow.h>
#include <jogasaki/executor/exchange/aggregate/flow.h>
#include <jogasaki/executor/exchange/forward/flow.h>

namespace jogasaki::executor::process::impl {

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

    explicit task_context(partition_index partition) :
        partition_(partition)
    {}

    task_context(partition_index partition,
        impl::ops::process_io_map const& process_io_map,
        std::unique_ptr<abstract::scan_info> scan_info
    ) :
        partition_(partition),
        process_io_map_(std::addressof(process_io_map)),
        scan_info_(std::move(scan_info))
    {}

    reader_container reader(reader_index idx) override {
        auto& flow = process_io_map_->input_at(idx)->data_flow_object();
        using step_kind = common::step_kind;
        switch(flow.kind()) {
            case step_kind::group:
                return static_cast<exchange::group::flow&>(flow).sources()[partition_].acquire_reader(); //NOLINT
            case step_kind::aggregate:
                return static_cast<exchange::aggregate::flow&>(flow).sources()[partition_].acquire_reader(); //NOLINT
            case step_kind::forward:
                return static_cast<exchange::forward::flow&>(flow).sources()[partition_].acquire_reader(); //NOLINT
            //TODO other exchanges
            default:
                fail();
        }
        return {};
    }

    record_writer* downstream_writer(writer_index idx) override {
        auto& flow = process_io_map_->output_at(idx)->data_flow_object();
        using step_kind = common::step_kind;
        switch(flow.kind()) {
            case step_kind::group:
                return &static_cast<exchange::group::flow&>(flow).sinks()[partition_].acquire_writer(); //NOLINT
            case step_kind::aggregate:
                return &static_cast<exchange::aggregate::flow&>(flow).sinks()[partition_].acquire_writer(); //NOLINT
            case step_kind::forward:
                return &static_cast<exchange::forward::flow&>(flow).sinks()[partition_].acquire_writer(); //NOLINT
            //TODO other exchanges
            default:
                fail();
        }
        return {};
    }

    record_writer* external_writer(writer_index idx) override {
        auto& p = process_io_map_->external_output_at(idx);
        using kind = ops::operator_kind;
        switch(p.kind()) {
            case kind::emit:
            case kind::write:
            default:
                break;
        }
        return {};
    }

    class abstract::scan_info const* scan_info() override {
        return {};
    }

private:
    std::size_t partition_{};
    impl::ops::process_io_map const* process_io_map_{};
    std::unique_ptr<abstract::scan_info> scan_info_{};
};

}


