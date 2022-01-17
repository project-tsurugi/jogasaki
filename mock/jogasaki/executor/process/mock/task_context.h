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
#include <jogasaki/executor/process/abstract/task_context.h>
#include <jogasaki/executor/reader_container.h>

#include "record_reader.h"
#include "record_writer.h"
#include "group_reader.h"
#include "external_writer.h"

namespace jogasaki::executor::process::mock {

class task_context : public abstract::task_context {
public:

    explicit task_context(
        std::vector<reader_container> readers = {},
        std::vector<std::shared_ptr<executor::record_writer>> downstream_writers = {},
        std::shared_ptr<executor::record_writer> external_writer = {},
        std::shared_ptr<abstract::scan_info> info = {}
    ) :
        readers_(std::move(readers)),
        downstream_writers_(std::move(downstream_writers)),
        external_writer_(std::move(external_writer)),
        scan_info_(std::move(info))
    {}

    reader_container reader(reader_index idx) override {
        return readers_.at(idx);
    }

    executor::record_writer* downstream_writer(writer_index idx) override {
        return downstream_writers_.at(idx).get();
    }

    executor::record_writer* external_writer() override {
        return external_writer_.get();
    }

    void do_release() {
        for(auto r : readers_) {
            r.release();
        }
        for(auto& w : downstream_writers_) {
            if (w) {
                w->release();
            }
        }
        if(external_writer_) {
            external_writer_->release();
            external_writer_.reset();
        }
        scan_info_.reset();
    }

    class abstract::scan_info const* scan_info() override {
        return nullptr;
    }

    [[nodiscard]] std::size_t partition() const noexcept {
        return partition_;
    }
private:
    std::size_t partition_{};
    std::vector<reader_container> readers_{};
    std::vector<std::shared_ptr<executor::record_writer>> downstream_writers_{};
    std::shared_ptr<executor::record_writer> external_writer_{};
    std::shared_ptr<abstract::scan_info> scan_info_{};
};

}
