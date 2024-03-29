/*
 * Copyright 2018-2023 Project Tsurugi.
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

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/model/task.h>
#include <jogasaki/model/step.h>
#include <jogasaki/executor/common/task.h>
#include <jogasaki/executor/io/group_reader.h>
#include <jogasaki/data/iterable_record_store.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/executor/process/mock/cogroup.h>

#include "../common/task_base.h"
#include "params.h"

namespace jogasaki::cogroup_cli {

using namespace jogasaki::executor;
using namespace jogasaki::executor::process;
using takatori::util::maybe_shared_ptr;

class priority_queue_consumer_task : public common_cli::task_base {
public:
    priority_queue_consumer_task(
            request_context* context,
            model::step* src,
            io::reader_container left_reader,
            io::reader_container right_reader,
            maybe_shared_ptr<meta::group_meta> l_meta,
            maybe_shared_ptr<meta::group_meta> r_meta
    ) :
            task_base(context, src),
            l_meta_(std::move(l_meta)),
            r_meta_(std::move(r_meta)),
            l_store_resource_(std::make_unique<memory::lifo_paged_memory_resource>(&global::page_pool())),
            l_store_varlen_resource_(std::make_unique<memory::lifo_paged_memory_resource>(&global::page_pool())),
            r_store_resource_(std::make_unique<memory::lifo_paged_memory_resource>(&global::page_pool())),
            r_store_varlen_resource_(std::make_unique<memory::lifo_paged_memory_resource>(&global::page_pool())),
            left_reader_(left_reader),  //NOLINT
            right_reader_(right_reader),  //NOLINT
            key_offset_(l_meta_->key().value_offset(0)),
            value_offset_(l_meta_->value().value_offset(0)),
            compare_info_(l_meta_->key()),
            key_comparator_(compare_info_),
            key_size_(l_meta_->key().record_size())
    {}

    void consume_member(io::group_reader* reader,
            std::size_t& record_counter,
            std::size_t& key_counter,
            std::unique_ptr<data::iterable_record_store>& store) {

        while(reader->next_member()) {
            DVLOG(log_trace) << *this << "   value : " << reader->get_member().get_value<double>(value_offset_);
            ++record_counter;
            total_val_ += reader->get_member().get_value<double>(value_offset_);

            auto rec = reader->get_member();
            store->append(rec);
        }
        key_counter++;
    }

    void execute() override {
        VLOG(log_debug) << *this << " consumer_task executed. count: " << count_;
        utils::get_watch().set_point(time_point_consume, id());
        key_offset_ = l_meta_->key().value_offset(0);
        value_offset_ = l_meta_->value().value_offset(0);

        auto* l_reader = left_reader_.reader<io::group_reader>();
        auto* r_reader = right_reader_.reader<io::group_reader>();
        using cogroup = process::mock::cogroup;
        cogroup cgrp{{l_reader, r_reader}, {l_meta_, r_meta_}};

        l_records_ = 0;
        r_records_ = 0;
        l_keys_ = 0;
        r_keys_ = 0;
        total_key_ = 0;
        total_val_ = 0;

        cogroup::consumer_type consumer = [&](accessor::record_ref key, std::vector<cogroup::iterator_pair>& values) {
            auto r_value_offset = r_meta_->value().value_offset(0);
            auto l_value_offset = l_meta_->value().value_offset(0);

            auto check_total = [&](std::int64_t key, double x, double y) {
                DVLOG(log_trace) << *this << " key: " << key << " value1 : " << x << " value2 : " << y;
                total_key_ += key;
                total_val_ += x + y;
            };

            if(values[0].first == values[0].second) {
                ++keys_right_only_;
                auto it = values[1].first;
                auto end = values[1].second;
                while(it != end) {
                    check_total(key.get_value<std::int64_t>(key_offset_) ,-1.0, (*it).get_value<double>(r_value_offset));
                    ++it;
                    ++values_right_only_;
                }
            } else if (values[1].first == values[1].second) {
                ++keys_left_only_;
                auto it = values[0].first;
                auto end = values[0].second;
                while(it != end) {
                    check_total(key.get_value<std::int64_t>(key_offset_) ,(*it).get_value<double>(l_value_offset), -1.0);
                    ++it;
                    ++values_left_only_;
                }
            } else {
                ++keys_matched_;
                auto l_it = values[0].first;
                auto l_end = values[0].second;
                auto r_end = values[1].second;
                while(l_it != l_end) {
                    auto r_it = values[1].first;
                    while(r_it != r_end) {
                        check_total(key.get_value<std::int64_t>(key_offset_) ,(*l_it).get_value<double>(l_value_offset), (*r_it).get_value<double>(r_value_offset));
                        ++r_it;
                        ++values_matched_;
                    }
                    ++l_it;
                }
            }
        };
        cgrp(consumer);

        utils::get_watch().set_point(time_point_consumed, id());
        LOG(INFO) << *this << " consumed "
                << "left (" << l_keys_ << " keys "<< l_records_ << " recs) "
                << "right (" << r_keys_ << " keys "<< r_records_ << " recs) "
                << "matched (" << keys_matched_ << " keys " << values_matched_ << " recs) "
                << "left only (" << keys_left_only_ << " keys " << values_left_only_ << " recs) "
                << "right only (" << keys_right_only_ << " keys " << values_right_only_ << " recs) "
                << "(sum: " << total_key_ << " " << total_val_ << ")";
    }

private:
    maybe_shared_ptr<meta::group_meta> l_meta_{};
    maybe_shared_ptr<meta::group_meta> r_meta_{};
    std::unique_ptr<memory::lifo_paged_memory_resource> l_store_resource_{};
    std::unique_ptr<memory::lifo_paged_memory_resource> l_store_varlen_resource_{};
    std::unique_ptr<memory::lifo_paged_memory_resource> r_store_resource_{};
    std::unique_ptr<memory::lifo_paged_memory_resource> r_store_varlen_resource_{};
    io::reader_container left_reader_{};
    io::reader_container right_reader_{};

    std::size_t key_offset_;
    std::size_t value_offset_;
    std::size_t l_records_ = 0;
    std::size_t r_records_ = 0;
    std::size_t l_keys_ = 0;
    std::size_t r_keys_ = 0;
    std::size_t total_key_ = 0;
    compare_info compare_info_{};
    comparator key_comparator_{};
    std::size_t key_size_ = 0;
    double total_val_ = 0;
    std::size_t keys_left_only_ = 0;
    std::size_t keys_right_only_ = 0;
    std::size_t keys_matched_ = 0;
    std::size_t values_left_only_ = 0;
    std::size_t values_right_only_ = 0;
    std::size_t values_matched_ = 0;

};

}



