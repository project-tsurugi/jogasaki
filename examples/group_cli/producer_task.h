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

#include <glog/logging.h>

#include <model/task.h>
#include <model/step.h>
#include <executor/common/task.h>
#include <channel.h>
#include <meta/record_meta.h>
#include "task_base.h"
#include "context.h"
#include "random.h"

namespace jogasaki::group_cli {

class producer_task : public task_base {
public:
    producer_task() = delete;
    producer_task(channel* channel,
            model::step* src,
            executor::exchange::sink* sink,
            std::shared_ptr<meta::record_meta> meta,
            context& c) :
            task_base(channel,  src),
            sink_(sink),
            meta_(std::move(meta)),
            context_(&c) {}

    void execute() override {
        DVLOG(1) << *this << " producer_task executed. count: " << count_;
        auto& watch = context_->watch_;
        watch->wrap(0);
        memory::page_pool pool{};
        memory::monotonic_paged_memory_resource resource{&pool};
        auto offset_c1 = meta_->value_offset(0);
        auto offset_c2 = meta_->value_offset(1);
        initialize_writer();
        xorshift_random rnd{};
        for(std::size_t i = 0; i < context_->records_per_upstream_partition_; ++i) {
            auto sz = meta_->record_size();
            auto ptr = resource.allocate(sz, meta_->record_alignment());
            auto ref = accessor::record_ref(ptr, sz);
            ref.set_value<std::int64_t>(offset_c1, rnd());
            ref.set_value<double>(offset_c2, rnd());
            writer_->write(ref);
        }
        watch->wrap(1);
        writer_->flush();
        watch->wrap(2);
    }

private:
    executor::exchange::sink* sink_{};
    std::shared_ptr<meta::record_meta> meta_{};
    executor::record_writer* writer_{};
    context* context_{};

    void initialize_writer() {
        if(!writer_) {
            writer_ = &sink_->acquire_writer();
        }
    }
};

}



