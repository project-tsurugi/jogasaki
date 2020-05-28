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

#include <memory>
#include <glog/logging.h>

#include <jogasaki/model/task.h>
#include <jogasaki/model/step.h>
#include <jogasaki/executor/common/task.h>
#include <jogasaki/executor/process/task_context.h>
#include <jogasaki/executor/reader_container.h>

#include "record_reader.h"
#include "record_writer.h"
#include "external_writer.h"

namespace jogasaki::executor::process::mock {

class task_context : public process::task_context {
public:
    task_context(
            std::shared_ptr<executor::record_reader> reader,
            std::shared_ptr<executor::record_writer> downstream_writer,
            std::shared_ptr<executor::record_writer> external_writer
            ) :
            reader_(std::move(reader)),
            downstream_writer_(std::move(downstream_writer)),
            external_writer_(std::move(external_writer))
    {}

    reader_container reader(reader_index idx) override {
        if (idx != 0) std::abort();
        return reader_container(reader_.get());
    }

    executor::record_writer* downstream_writer(writer_index idx) override {
        if (idx != 0) std::abort();
        return downstream_writer_.get();
    }

    executor::record_writer* external_writer(writer_index idx) override {
        if (idx != 0) std::abort();
        return external_writer_.get();
    }

    void do_release() override {
        if (reader_) reader_->release();
        if (downstream_writer_) downstream_writer_->release();
        if (external_writer_) external_writer_->release();
    }

    class scan_info const* scan_info() override {
        return nullptr;
    }

private:
    std::shared_ptr<executor::record_reader> reader_{};
    std::shared_ptr<executor::record_writer> downstream_writer_{};
    std::shared_ptr<executor::record_writer> external_writer_{};
};

}
