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

#include <jogasaki/model/task.h>
#include <jogasaki/model/step.h>
#include <jogasaki/executor/common/task.h>
#include <jogasaki/meta/record_meta.h>

#include <jogasaki/mock/mock_task.h>
#include <jogasaki/test_root.h>

namespace jogasaki::executor {

class simple_scan_process_task : public mock_task {
public:
    simple_scan_process_task() = delete;
    ~simple_scan_process_task() override = default;
    simple_scan_process_task(simple_scan_process_task&& other) noexcept = default;
    simple_scan_process_task& operator=(simple_scan_process_task&& other) noexcept = default;
    simple_scan_process_task(request_context* context, model::step* src, record_writer* writer) : mock_task(context,  src), writer_(writer) {}

    record_writer* writer_{};

    void execute() override {
        LOG(INFO) << *this << " simple_scan_process_main_task executed. count: " << count_;

        auto rec_meta = test_root::test_record_meta1();
        exchange::group::group_info info{rec_meta, {1}};
        auto key_meta = info.key_meta();

        memory::page_pool pool{};
        memory::monotonic_paged_memory_resource resource{&pool};
        auto offset_c1 = rec_meta->value_offset(0);
        auto offset_c2 = rec_meta->value_offset(1);
        for(std::size_t i = 0; i < 3; ++i) {
            auto sz = rec_meta->record_size();
            auto ptr = resource.allocate(sz, rec_meta->record_alignment());
            auto ref = accessor::record_ref(ptr, sz);
            ref.set_value<std::int64_t>(offset_c1, i);
            ref.set_value<double>(offset_c2, i);
            writer_->write(ref);
        }
//        writer_->flush();
    }
};

}



