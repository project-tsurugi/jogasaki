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
#include <jogasaki/executor/exchange/step.h>
#include <jogasaki/executor/exchange/group/flow.h>
#include <jogasaki/executor/exchange/forward/flow.h>

namespace jogasaki::executor::process::impl {

struct reader_info {
    executor::exchange::step* step_{};
    reader_container reader_{};
    bool reader_acquired_{};
};

struct writer_info {
    executor::exchange::step* step_{};
    record_writer* writer_{};
    bool writer_acquired_{};
};

struct external_writer_info {
    record_writer* writer_{};
    bool writer_acquired_{};
};

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
        std::vector<reader_info> readers,
        std::vector<writer_info> writers,
        std::vector<writer_info> external_writers,
        std::unique_ptr<abstract::scan_info> scan_info
    ) :
        partition_(partition),
        readers_(std::move(readers)),
        writers_(std::move(writers)),
        external_writers_(std::move(external_writers)),
        scan_info_(std::move(scan_info))
    {}

    reader_container reader(reader_index idx) override {
        auto& info = readers_[idx];
        if (info.reader_acquired_) {
            return info.reader_;
        }
        auto& flow = info.step_->data_flow_object();
        using step_kind = common::step_kind;
        switch(flow.kind()) {
            case step_kind::group: {
                auto r = static_cast<exchange::group::flow&>(flow).sources()[partition_].acquire_reader(); //NOLINT
                info.reader_ = r;
                return r;
            }
            case step_kind::forward: {
                auto r = static_cast<exchange::forward::flow&>(flow).sources()[partition_].acquire_reader(); //NOLINT
                info.reader_ = r;
                return r;
            }
            //TODO other exchanges
            default:
                std::abort();
        }
        return {};
    }

    record_writer* downstream_writer(writer_index idx) override {
        auto& info = writers_[idx];
        if (info.writer_acquired_) {
            return info.writer_;
        }
        auto& flow = info.step_->data_flow_object();
        using step_kind = common::step_kind;
        switch(flow.kind()) {
            case step_kind::group: {
                auto w = &static_cast<exchange::group::flow&>(flow).sinks()[partition_].acquire_writer(); //NOLINT
                info.writer_ = w;
                return w;
            }
            case step_kind::forward: {
                auto w = &static_cast<exchange::forward::flow&>(flow).sinks()[partition_].acquire_writer(); //NOLINT
                info.writer_ = w;
                return w;
            }
            //TODO other exchanges
            default:
                std::abort();
        }
        return {};
    }

    record_writer* external_writer(writer_index idx) override {
        (void)idx;
        return {};
    }

    class abstract::scan_info const* scan_info() override {
        return {};
    }

protected:
    void do_release() override {

    }

private:
    std::size_t partition_{};
    std::vector<reader_info> readers_{};
    std::vector<writer_info> writers_{};
    std::vector<writer_info> external_writers_{};
    std::unique_ptr<abstract::scan_info> scan_info_{};
};

}


