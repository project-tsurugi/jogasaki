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
#include <jogasaki/executor/group_reader.h>
#include "../common/task_base.h"
#include "params.h"
#include "../common/cli_constants.h"

namespace jogasaki::cogroup_cli {

using namespace jogasaki::executor;

class consumer_task : public common_cli::task_base {
public:
    consumer_task() = default;
    consumer_task(
            std::shared_ptr<request_context> context,
            model::step* src,
            executor::reader_container left_reader,
            executor::reader_container right_reader,
            std::shared_ptr<meta::group_meta> meta,
            params& c
    ) :
            task_base(std::move(context), src),
            meta_(std::move(meta)),
            left_reader_(left_reader),
            right_reader_(right_reader),
            params_(&c) {}

    void consume_member(group_reader* reader, std::size_t& record_counter) {
        while(reader->next_member()) {
            DVLOG(2) << *this << "   value : " << reader->get_member().get_value<double>(value_offset_);
            ++record_counter;
            total_val_ += reader->get_member().get_value<double>(value_offset_);
        }
    }

    void consume(group_reader* reader, std::size_t& record_counter, std::size_t& key_counter) {
        while(reader->next_group()) {
            DVLOG(2) << *this << " key : " << reader->get_group().get_value<std::int64_t>(key_offset_);
            total_key_ += reader->get_group().get_value<std::int64_t>(key_offset_);
            ++key_counter;
            consume_member(reader, record_counter);
        }
    }

    void execute() override {
        VLOG(1) << *this << " consumer_task executed. count: " << count_;
        auto& watch = utils::watch_;
        watch->set_point(time_point_consume, id());
        key_offset_ = meta_->key().value_offset(0);
        value_offset_ = meta_->value().value_offset(0);
        auto* l_reader = left_reader_.reader<executor::group_reader>();
        auto* r_reader = right_reader_.reader<executor::group_reader>();
        l_records_ = 0;
        r_records_ = 0;
        l_keys_ = 0;
        r_keys_ = 0;
        total_key_ = 0;
        total_val_ = 0;
        enum class state {
            init,
            left_members,
            right_members,
            eof_left,
            eof_right,
            eof
        };
        state s{state::init};
        while(s != state::eof) {
            switch(s) {
                case state::init:
                    if(!l_reader->next_group()) {
                        s = state::eof_left;
                        break;
                    }
                    s = state::left_members;
                    break;
                case state::left_members:
//                    consume_member(l_reader, l_records_);
                    s = state::eof;
                    break;
                case state::eof_left:
//                    consume(r_reader, r_records_, r_keys_);
                    s = state::eof;
                    break;
                case state::eof_right:
//                    consume(l_reader, l_records_, l_keys_);
                    s = state::eof;
                    break;
                default:
                    break;
            }
        }
        l_reader->release();
        r_reader->release();
        watch->set_point(time_point_consumed, id());
        LOG(INFO) << *this << " consumed"
                << "left " << l_records_ << " records with unique "<< l_keys_ << " keys "
                << "right " << r_records_ << " records with unique "<< r_keys_ << " keys "
                << "(sum: " << total_key_ << " " << total_val_ << ")";
    }

private:
    std::shared_ptr<meta::group_meta> meta_{};
    executor::reader_container left_reader_{};
    executor::reader_container right_reader_{};
    params* params_{};
    std::size_t key_offset_;
    std::size_t value_offset_;
    std::size_t l_records_ = 0;
    std::size_t r_records_ = 0;
    std::size_t l_keys_ = 0;
    std::size_t r_keys_ = 0;
    std::size_t total_key_ = 0;
    double total_val_ = 0;
};

}



