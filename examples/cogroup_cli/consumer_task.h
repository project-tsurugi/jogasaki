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
#include <jogasaki/data/iteratable_record_store.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/utils/aligned_unique_ptr.h>

#include "../common/task_base.h"
#include "params.h"
#include "../common/cli_constants.h"

namespace jogasaki::cogroup_cli {

using namespace jogasaki::executor;

class consumer_task : public common_cli::task_base {
public:
    consumer_task(
            std::shared_ptr<request_context> context,
            model::step* src,
            executor::reader_container left_reader,
            executor::reader_container right_reader,
            std::shared_ptr<meta::group_meta> l_meta,
            std::shared_ptr<meta::group_meta> r_meta,
            params& c
    ) :
            task_base(std::move(context), src),
            l_meta_(std::move(l_meta)),
            r_meta_(std::move(r_meta)),
            l_store_resource_(std::make_unique<memory::lifo_paged_memory_resource>(&global::global_page_pool)),
            l_store_varlen_resource_(std::make_unique<memory::lifo_paged_memory_resource>(&global::global_page_pool)),
            r_store_resource_(std::make_unique<memory::lifo_paged_memory_resource>(&global::global_page_pool)),
            r_store_varlen_resource_(std::make_unique<memory::lifo_paged_memory_resource>(&global::global_page_pool)),
            l_store_(std::make_unique<data::iteratable_record_store>(
                    l_store_resource_.get(),
                    l_store_varlen_resource_.get(),
                    l_meta_->value_shared()
            )),
            r_store_(std::make_unique<data::iteratable_record_store>(
                    r_store_resource_.get(),
                    r_store_varlen_resource_.get(),
                    r_meta_->value_shared()
            )),
            left_reader_(left_reader),  //NOLINT
            right_reader_(right_reader),  //NOLINT
            l_key_(utils::make_aligned_array<char>(l_meta_->key().record_alignment(), l_meta_->key().record_size())),
            r_key_(utils::make_aligned_array<char>(r_meta_->key().record_alignment(), r_meta_->key().record_size())),
            params_(&c),
            key_offset_(l_meta_->key().value_offset(0)),
            value_offset_(l_meta_->value().value_offset(0)),
            key_comparator_(l_meta_->key_shared().get()),
            key_size_(l_meta_->key().record_size())
    {}

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

    void next_left_key() {
        auto* l_reader = left_reader_.reader<executor::group_reader>();
        auto key = l_reader->get_group();
        memcpy(l_key_.get(), key.data(), key.size());
    }
    void next_right_key() {
        auto* r_reader = right_reader_.reader<executor::group_reader>();
        auto key = r_reader->get_group();
        memcpy(r_key_.get(), key.data(), key.size());
    }

    void execute() override {
        VLOG(1) << *this << " consumer_task executed. count: " << count_;
        utils::get_watch().set_point(time_point_consume, id());
        key_offset_ = l_meta_->key().value_offset(0);
        value_offset_ = l_meta_->value().value_offset(0);
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
            did_read_left_key,
            did_read_both_key,
            on_left_member,
            on_right_member,
            left_eof,
            filled,
            both_consumed,
            end,
        };

        enum class read {
            none,
            left,
            right,
            both,
        };
        bool left_eof = false;
        bool right_eof = false;
        state s{state::init};
        read r{read::none};
        while(s != state::end) {
            switch(s) {
                case state::init:
                case state::both_consumed: {
                    if(!l_reader->next_group()) {
                        left_eof = true;
                        s = state::left_eof;
                        break;
                    }
                    next_left_key();
                    s = state::did_read_left_key;
                    break;
                }
                case state::did_read_left_key: {
                    if(!r_reader->next_group()) {
                        right_eof = true;
                        s = state::on_left_member;
                        break;
                    }
                    next_right_key();
                    s = state::did_read_both_key;
                    break;
                }
                case state::did_read_both_key: {
                    if (auto c = key_comparator_(accessor::record_ref(l_key_.get(), key_size_),accessor::record_ref(r_key_.get(), key_size_)); c < 0) {
                        r = read::left;
                        s = state::on_left_member;
                    } else if (c > 0) {
                        r = read::right;
                        s = state::on_right_member;
                    } else {
                        r = read::both;
                        s = state::on_left_member;
                    }
                    break;
                }
                case state::on_left_member:
                    consume_member(l_reader, l_records_);
                    if (r == read::both) {
                        s = state::on_right_member;
                    } else {
                        // send data
                        s = state::filled;
                    }
                    break;
                case state::on_right_member:
                    consume(r_reader, r_records_, r_keys_);
                    // send data
                    s = state::filled;
                    break;
                case state::filled:
                    if (r == read::both) {
                        r = read::none;
                        s = state::both_consumed;
                        break;
                    }
                    if (r == read::none) {
                        if (left_eof) {
                            if(!r_reader->next_group()) {
                                right_eof = true;
                                s = state::end;
                                break;
                            }
                            next_right_key();
                            s = state::on_right_member;
                            break;
                        }
                        if (right_eof) {
                            if(!l_reader->next_group()) {
                                left_eof = true;
                                s = state::end;
                                break;
                            }
                            next_left_key();
                            s = state::on_left_member;
                            break;
                        }
                        std::abort();
                    }
                    if (r == read::left) {
                        if(!l_reader->next_group()) {
                            left_eof = true;
                            s = state::on_right_member;
                            break;
                        }
                        next_left_key();
                        s = state::did_read_both_key;
                    }
                    if (r == read::right) {
                        if(!r_reader->next_group()) {
                            right_eof = true;
                            s = state::on_left_member;
                            break;
                        }
                        next_right_key();
                        s = state::did_read_both_key;
                    }
                    std::abort();
                default:
                    std::abort();
            }
        }
        l_reader->release();
        r_reader->release();
        utils::get_watch().set_point(time_point_consumed, id());
        LOG(INFO) << *this << " consumed "
                << "left " << l_records_ << " records with unique "<< l_keys_ << " keys "
                << "right " << r_records_ << " records with unique "<< r_keys_ << " keys "
                << "(sum: " << total_key_ << " " << total_val_ << ")";
    }

private:
    std::shared_ptr<meta::group_meta> l_meta_{};
    std::shared_ptr<meta::group_meta> r_meta_{};
    std::unique_ptr<memory::lifo_paged_memory_resource> l_store_resource_{};
    std::unique_ptr<memory::lifo_paged_memory_resource> l_store_varlen_resource_{};
    std::unique_ptr<memory::lifo_paged_memory_resource> r_store_resource_{};
    std::unique_ptr<memory::lifo_paged_memory_resource> r_store_varlen_resource_{};
    std::unique_ptr<data::iteratable_record_store> l_store_{};
    std::unique_ptr<data::iteratable_record_store> r_store_{};
    executor::reader_container left_reader_{};
    executor::reader_container right_reader_{};
    utils::aligned_array<char> l_key_;
    utils::aligned_array<char> r_key_;
    params* params_{};

    std::size_t key_offset_;
    std::size_t value_offset_;
    std::size_t l_records_ = 0;
    std::size_t r_records_ = 0;
    std::size_t l_keys_ = 0;
    std::size_t r_keys_ = 0;
    std::size_t total_key_ = 0;
    comparator key_comparator_{};
    std::size_t key_size_ = 0;
    double total_val_ = 0;
};

}



