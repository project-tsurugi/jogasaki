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
#include <jogasaki/executor/process/processor_context.h>
#include <jogasaki/executor/reader_container.h>
#include <jogasaki/channel.h>
#include <jogasaki/utils.h>

namespace jogasaki::executor::process {

class mock_processor_context : public processor_context {
public:

    reader_container reader(reader_index idx) override {
        if (idx != 0) std::abort();
        return reader_container(reader_.get());
    }

    record_writer* downstream_writer(writer_index idx) override {
        if (idx != 0) std::abort();
        return downstream_writer_.get();
    }

    record_writer* external_writer(writer_index idx) override {
        if (idx != 0) std::abort();
        return external_writer_.get();
    }

private:
    std::unique_ptr<record_reader> reader_{};
    std::unique_ptr<record_writer> downstream_writer_{};
    std::unique_ptr<record_writer> external_writer_{};
};

}
