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

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/model/task.h>
#include <jogasaki/model/step.h>
#include <jogasaki/executor/common/task.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/utils/performance_tools.h>
#include <jogasaki/utils/random.h>
#include "task_base.h"
#include "cli_constants.h"

namespace jogasaki::common_cli {

using takatori::util::maybe_shared_ptr;

template <class Params>
class producer_task : public task_base {
public:
    producer_task() = delete;
    producer_task(request_context* context,
            model::step* src,
            executor::exchange::sink* sink,
            maybe_shared_ptr<meta::record_meta> meta,
            Params& c,
            memory::monotonic_paged_memory_resource& resource
    ) :
            task_base(std::move(context),  src),
            sink_(sink),
            meta_(std::move(meta)),
            params_(&c),
            resource_(&resource)
    {}

    void execute() override {
        VLOG(1) << *this << " producer_task executed. count: " << count_;
        utils::get_watch().set_point(time_point_prepare, id());
        LOG(INFO) << id() << " start prepare";
        initialize_writer();
        std::vector<std::pair<void*, void*>> continuous_ranges{}; // bunch of records are separated to multiple continuous regions
        prepare_data(continuous_ranges);
        utils::get_watch().set_point(time_point_produce, id());
        LOG(INFO) << id() << " start produce";
        produce_data(continuous_ranges);
        writer_->flush();
        writer_->release();
        utils::get_watch().set_point(time_point_produced, id());
        LOG(INFO) << id() << " end produce";
    }

private:
    executor::exchange::sink* sink_{};
    maybe_shared_ptr<meta::record_meta> meta_{};
    executor::record_writer* writer_{};
    Params* params_{};
    memory::monotonic_paged_memory_resource* resource_{};

    void initialize_writer() {
        if(!writer_) {
            writer_ = &sink_->acquire_writer();
        }
    }

    void prepare_data(std::vector<std::pair<void*, void*>>& continuous_ranges) {
        utils::xorshift_random64 rnd{static_cast<std::uint64_t>(id()+1)};
        auto sz = meta_->record_size();
        auto recs_per_page = memory::page_size / sizeof(void*);
        auto records = params_->records_per_upstream_partition_;
        continuous_ranges.reserve( ( records + recs_per_page - 1)/recs_per_page);
        void* prev = nullptr;
        void* begin_range = nullptr;
        for(std::size_t i = 0; i < records; ++i) {
            auto ptr = resource_->allocate(sz, meta_->record_alignment());
            if (prev == nullptr) {
                begin_range = ptr;
            } else if (ptr != static_cast<char*>(prev) + sz) { //NOLINT
                continuous_ranges.emplace_back(begin_range, prev);
                begin_range = ptr;
            }
            prev = ptr;
            auto ref = accessor::record_ref(ptr, sz);

            for(std::size_t j=0, n=meta_->field_count(); j < n; ++j) {
                auto offset = meta_->value_offset(j);
                auto&& f = meta_->at(j);
                using kind = meta::field_type_kind;
                switch(f.kind()) {
                    case kind::int8: {
                        auto val = params_->sequential_data_ ? i : rnd();
                        std::int64_t c1 = (params_->key_modulo_ == static_cast<std::size_t>(-1)) ? val : (val % params_->key_modulo_);
                        c1 = (c1 > 0) ? c1 : -c1;
                        ref.set_value<std::int64_t>(offset, c1);
                        break;
                    }
                    case kind::float8: {
                        double c2 = rnd();
                        ref.set_value<double>(offset, c2);
                        break;
                    }
                    case kind::character: {
                        bool sequential = params_->sequential_data_;
                        char c = 'A' + (sequential ? i : rnd()) % 26;
                        std::size_t len = 1 + (sequential ? i : rnd()) % 70;
                        len = i % 2 == 1 ? len + 20 : len;
                        std::string d(len, c);
                        ref.set_value<accessor::text>(offset, accessor::text{resource_, d.data(), d.size()});
                        break;
                    }
                    default:
                        break;
                }
                if (meta_->nullable(j)) {
                    auto nullity_offset = meta_->nullity_offset(j);
                    ref.set_null(nullity_offset, false);
                }
            }
        }
        if(begin_range) {
            continuous_ranges.emplace_back(begin_range, prev);
        }
    }
    void produce_data(std::vector<std::pair<void*, void*>>& continuous_ranges) {
        auto sz = meta_->record_size();
        for(auto range : continuous_ranges) {
            for(char* p = static_cast<char*>(range.first); p <= range.second; p += sz) { //NOLINT
                auto ref = accessor::record_ref(p, sz);
                writer_->write(ref);
            }
        }
    }
};

}



