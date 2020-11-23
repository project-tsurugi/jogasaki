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
#include <jogasaki/executor/process/external_writer.h>
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

    using result_stores = request_context::result_stores;
    /**
     * @brief create new empty instance
     */
    task_context() = default;

    explicit task_context(partition_index partition) :
        partition_(partition)
    {}

    /**
     * @brief create new object
     * @param partition the index of partition assigned to this object
     * @param io_exchange_map mapping from input/output indices to exchanges
     * @param scan_info the scan information, nullptr if the task doesn't contain scan
     * @param stores the stores to keep the result data
     * @param external_output_record_resource the memory resource backing the result store
     * @param external_output_varlen_resource the memory resource backing the result store
     */
    task_context(partition_index partition,
        io_exchange_map const& io_exchange_map,
        std::shared_ptr<impl::scan_info> scan_info,
        result_stores* stores,
        memory::paged_memory_resource* external_output_record_resource = {},
        memory::paged_memory_resource* external_output_varlen_resource = {}
    ) :
        partition_(partition),
        io_exchange_map_(std::addressof(io_exchange_map)),
        scan_info_(std::move(scan_info)),
        stores_(stores),
        external_writers_(io_exchange_map_->external_output_count()),
        external_output_record_resource_(external_output_record_resource),
        external_output_varlen_resource_(external_output_varlen_resource)
    {}

    reader_container reader(reader_index idx) override {
        auto& flow = io_exchange_map_->input_at(idx)->data_flow_object();
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
        auto& flow = io_exchange_map_->output_at(idx)->data_flow_object();
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
        BOOST_ASSERT(idx < external_writers_.size());  //NOLINT
        BOOST_ASSERT(stores_ != nullptr);  //NOLINT
        auto& op = unsafe_downcast<ops::emit>(io_exchange_map_->external_output_at(idx));
        auto& slot = external_writers_.operator[](idx);
        if (! slot) {
            auto& st = stores_->operator[](idx) = std::make_shared<data::iterable_record_store>(
                external_output_record_resource_,
                external_output_varlen_resource_,
                op.meta()
            );
            slot = std::make_shared<class external_writer>(*st, op.meta());
        }
        return slot.get();
    }

    class abstract::scan_info const* scan_info() override {
        return scan_info_.get();
    }

    [[nodiscard]] std::size_t partition() const noexcept {
        return partition_;
    }

private:
    std::size_t partition_{};
    io_exchange_map const* io_exchange_map_{};
    std::shared_ptr<impl::scan_info> scan_info_{};
    result_stores* stores_{};
    std::vector<std::shared_ptr<class external_writer>> external_writers_{};
    memory::paged_memory_resource* external_output_record_resource_{};
    memory::paged_memory_resource* external_output_varlen_resource_{};
};

}


