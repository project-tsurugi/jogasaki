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
            context& c,
            memory::monotonic_paged_memory_resource& resource
            ) :
            task_base(channel,  src),
            sink_(sink),
            meta_(std::move(meta)),
            context_(&c),
            resource_(&resource)
            {}
    void execute() override {
        DVLOG(1) << *this << " producer_task executed. count: " << count_;
        auto& watch = context_->watch_;
        watch->wrap(0);
        initialize_writer();
        std::vector<std::pair<void*, void*>> continuous_ranges{}; // bunch of records are separated to multiple continuous regions
        prepare_data(continuous_ranges);
        watch->wrap(1);
        produce_data(continuous_ranges);
        watch->wrap(2);
        writer_->flush();
        writer_->release();
        watch->wrap(3);
    }

private:
    executor::exchange::sink* sink_{};
    std::shared_ptr<meta::record_meta> meta_{};
    executor::record_writer* writer_{};
    context* context_{};
    memory::monotonic_paged_memory_resource* resource_{};

    void initialize_writer() {
        if(!writer_) {
            writer_ = &sink_->acquire_writer();
        }
    }

    void prepare_data(std::vector<std::pair<void*, void*>>& continuous_ranges) {
        auto offset_c1 = meta_->value_offset(0);
        auto offset_c2 = meta_->value_offset(1);
        xorshift_random rnd{};
        auto sz = meta_->record_size();
        auto recs_per_page = memory::page_size / sizeof(void*);
        auto partitions = context_->records_per_upstream_partition_;
        continuous_ranges.reserve( ( partitions + recs_per_page - 1)/recs_per_page);
        void* prev = nullptr;
        void* begin_range = nullptr;
        for(std::size_t i = 0; i < partitions; ++i) {
            auto ptr = resource_->allocate(sz, meta_->record_alignment());
            if (prev == nullptr) {
                begin_range = ptr;
            } else if (ptr != static_cast<char*>(prev) + sz) {
                continuous_ranges.emplace_back(begin_range, prev);
                begin_range = ptr;
            }
            prev = ptr;
            auto ref = accessor::record_ref(ptr, sz);
            ref.set_value<std::int64_t>(offset_c1, rnd());
            ref.set_value<double>(offset_c2, rnd());
        }
        if(begin_range) {
            continuous_ranges.emplace_back(begin_range, prev);
        }
    }
    void produce_data(std::vector<std::pair<void*, void*>>& continuous_ranges) {
        auto sz = meta_->record_size();
        for(auto range : continuous_ranges) {
            for(char* p = static_cast<char*>(range.first); p <= range.second; p += sz) {
                auto ref = accessor::record_ref(p, sz);
                writer_->write(ref);
            }
        }
    }
};

}



