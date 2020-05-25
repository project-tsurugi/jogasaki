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
#include "task_base.h"
#include <cli_constants.h>

namespace jogasaki::group_cli {

class consumer_task : public task_base {
public:
    consumer_task() = default;
    consumer_task(
            std::shared_ptr<request_context> context,
            model::step* src,
            executor::reader_container reader,
            std::shared_ptr<meta::group_meta> meta,
            params& c
    ) : task_base(std::move(context), src), meta_(std::move(meta)), reader_(reader), params_(&c) {}

    void execute() override {
        VLOG(1) << *this << " consumer_task executed. count: " << count_;
        auto& watch = params_->watch_;
        watch->set_point(time_point_consume, id());
        auto key_offset = meta_->key().value_offset(0);
        auto value_offset = meta_->value().value_offset(0);
        auto* reader = reader_.reader<executor::group_reader>();
        std::size_t records = 0;
        std::size_t keys = 0;
        std::size_t total_key = 0;
        double total_val = 0;
        while(reader->next_group()) {
            DVLOG(2) << *this << " key : " << reader->get_group().get_value<std::int64_t>(key_offset);
            total_key += reader->get_group().get_value<std::int64_t>(key_offset);
            ++keys;
            while(reader->next_member()) {
                DVLOG(2) << *this << "   value : " << reader->get_member().get_value<double>(value_offset);
                ++records;
                total_val += reader->get_member().get_value<double>(value_offset);
            }
        }
        reader->release();
        watch->set_point(time_point_consumed, id());
        LOG(INFO) << *this << " consumed " << records << " records with unique "<< keys << " keys (sum: " << total_key << " " << total_val << ")";
    }

private:
    std::shared_ptr<meta::group_meta> meta_{};
    executor::reader_container reader_{};
    params* params_{};
};

}



