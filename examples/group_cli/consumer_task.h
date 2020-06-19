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
#include <jogasaki/utils/aligned_unique_ptr.h>
#include "../common/task_base.h"
#include "../common/aggregator.h"
#include "../common/cli_constants.h"

namespace jogasaki::group_cli {

class consumer_task : public common_cli::task_base {
public:
    consumer_task() = default;
    consumer_task(
            std::shared_ptr<request_context> context,
            model::step* src,
            executor::reader_container reader,
            std::shared_ptr<meta::group_meta> meta,
            params& c
    ) : task_base(std::move(context), src), meta_(std::move(meta)), reader_(reader), params_(&c) {}

    void consume_record(executor::group_reader* reader) {
        while(reader->next_group()) {
            DVLOG(2) << *this << " key : " << reader->get_group().get_value<std::int64_t>(key_offset_);
            total_key_ += reader->get_group().get_value<std::int64_t>(key_offset_);
            ++keys_;
            while(reader->next_member()) {
                DVLOG(2) << *this << "   value : " << reader->get_member().get_value<double>(value_offset_);
                ++records_;
                total_val_ += reader->get_member().get_value<double>(value_offset_);
            }
        }
    }

    void aggregate_group(executor::group_reader* reader) {
        auto aggregator = common_cli::create_aggregator();
        auto key_size = meta_->key().record_size();
        auto value_size = meta_->value().record_size();
        utils::aligned_array<char> key = utils::make_aligned_array<char>(meta_->key().record_alignment(), key_size);
        utils::aligned_array<char> value = utils::make_aligned_array<char>(meta_->value().record_alignment(), value_size);
        accessor::record_copier key_copier{meta_->key_shared()};
        accessor::record_ref value_ref(value.get(), value_size);
        accessor::record_ref key_ref(key.get(), key_size);

        while(reader->next_group()) {
            key_copier(key_ref, reader->get_group());
            value_ref.set_value(meta_->value().value_offset(0), 0.0);
            while(reader->next_member()) {
                (*aggregator)(&meta_->value(), value_ref, reader->get_member());
            }
            total_key_ += key_ref.get_value<std::int64_t>(key_offset_);
            total_val_ += value_ref.get_value<double>(value_offset_);
            ++keys_;
            ++records_;
            DVLOG(2) << *this << " key : " << key_ref.get_value<std::int64_t>(key_offset_);
            DVLOG(2) << *this << "   value : " << value_ref.get_value<double>(value_offset_);
        }
    }
    void execute() override {
        VLOG(1) << *this << " consumer_task executed. count: " << count_;
        //        utils::get_watch().set_point(time_point_consume, id());
        watch->set_point(time_point_consume, id());
        auto* reader = reader_.reader<executor::group_reader>();
        key_offset_ = meta_->key().value_offset(0);
        value_offset_ = meta_->value().value_offset(0);
        records_ = 0;
        total_val_ = 0.0;
        total_key_ = 0;
        keys_ = 0;
        if (params_->aggregate_group_) {
            aggregate_group(reader);
        } else {
            consume_record(reader);
        }
        reader->release();
        //        utils::get_watch().set_point(time_point_consumed, id());
        watch->set_point(time_point_consumed, id());
        LOG(INFO) << *this << " consumed " << records_ << " records with unique "<< keys_ << " keys (sum: " << total_key_ << " " << total_val_ << ")";
    }

private:
    std::shared_ptr<meta::group_meta> meta_{};
    executor::reader_container reader_{};
    params* params_{};
    std::size_t key_offset_{};
    std::size_t value_offset_{};
    std::size_t keys_{};
    std::size_t total_key_{};
    std::size_t records_{};
    double total_val_{};
};

}

