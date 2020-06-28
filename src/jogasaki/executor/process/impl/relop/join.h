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
#include <jogasaki/meta/group_meta.h>
#include <jogasaki/executor/common/task.h>
#include <jogasaki/executor/group_reader.h>
#include <jogasaki/executor/reader_container.h>
#include <jogasaki/data/iteratable_record_store.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/executor/comparator.h>
#include <jogasaki/executor/global.h>

#include "../iterator_pair.h"

namespace jogasaki::executor::process::impl::relop {

using namespace jogasaki::executor;

class join {
public:
    using input_index = std::size_t;

    join(
            std::shared_ptr<meta::record_meta> key_meta,
            std::vector<std::shared_ptr<meta::record_meta>> records_meta,
            std::function<void(accessor::record_ref)> downstream
    ) :
            key_meta_(std::move(key_meta)),
            records_meta_(std::move(records_meta)),
            downstream_(std::move(downstream))
    {}

    void operator()(accessor::record_ref key, std::vector<iterator_pair>& groups) {
        (void)key;
        (void)groups;

        /*
        auto r_value_len = r_meta_->value().record_size();
        auto r_value_offset = r_meta_->value().value_offset(0);
        auto l_value_len = l_meta_->value().record_size();
        auto l_value_offset = l_meta_->value().value_offset(0);
        if(l_store_->empty()) {
            ++keys_right_only_;
            auto it = r_store_->begin();
            auto end = r_store_->end();
            DVLOG(2) << *this << " key : " << reinterpret_cast<std::int64_t>(r_key_.get());
            while(it != end) {
                auto rec = accessor::record_ref(*it, r_value_len);
                consumer(reinterpret_cast<std::int64_t>(l_key_.get()) ,-1.0, rec.get_value<double>(r_value_offset));
                ++it;
                ++values_right_only_;
            }
        } else if (r_store_->empty()) {
            ++keys_left_only_;
            auto it = l_store_->begin();
            auto end = l_store_->end();
            DVLOG(2) << *this << " key : " << reinterpret_cast<std::int64_t>(l_key_.get());
            while(it != end) {
                auto rec = accessor::record_ref(*it, l_value_len);
                consumer(reinterpret_cast<std::int64_t>(r_key_.get()) ,rec.get_value<double>(l_value_offset), -1.0);
                ++it;
                ++values_left_only_;
            }
        } else {
            ++keys_matched_;
            auto l_it = l_store_->begin();
            auto l_end = l_store_->end();
            auto r_end = r_store_->end();
            DVLOG(2) << *this << " key : " << reinterpret_cast<std::int64_t>(l_key_.get());
            while(l_it != l_end) {
                auto r_it = r_store_->begin();
                while(r_it != r_end) {
                    auto l_rec = accessor::record_ref(*l_it, l_value_len);
                    auto r_rec = accessor::record_ref(*r_it, r_value_len);
                    consumer(reinterpret_cast<std::int64_t>(r_key_.get()) ,l_rec.get_value<double>(l_value_offset), r_rec.get_value<double>(r_value_offset));
                    ++r_it;
                    ++values_matched_;
                }
                ++l_it;
            }
        }
         */
    }

private:
    std::shared_ptr<meta::record_meta> key_meta_{};
    std::vector<std::shared_ptr<meta::record_meta>> records_meta_{};
    std::function<void(accessor::record_ref)> downstream_{};
};

}
