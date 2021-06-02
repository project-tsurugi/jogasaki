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

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/model/task.h>
#include <jogasaki/model/step.h>
#include <jogasaki/executor/common/task.h>
#include <jogasaki/executor/group_reader.h>
#include <jogasaki/data/iterable_record_store.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/data/small_record_store.h>

#include "../common/task_base.h"
#include "params.h"
#include "cli_constants.h"

namespace jogasaki::cogroup_cli {

using namespace jogasaki::executor;
using takatori::util::maybe_shared_ptr;

class consumer_task : public common_cli::task_base {
public:
    consumer_task(
            request_context* context,
            model::step* src,
            executor::reader_container left_reader,
            executor::reader_container right_reader,
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
            l_store_(std::make_unique<data::iterable_record_store>(
                    l_store_resource_.get(),
                    l_store_varlen_resource_.get(),
                    l_meta_->value_shared()
            )),
            r_store_(std::make_unique<data::iterable_record_store>(
                    r_store_resource_.get(),
                    r_store_varlen_resource_.get(),
                    r_meta_->value_shared()
            )),
            left_reader_(left_reader),  //NOLINT
            right_reader_(right_reader),  //NOLINT
            l_key_(l_meta_->key_shared()),
            r_key_(r_meta_->key_shared()),
            key_offset_(l_meta_->key().value_offset(0)),
            value_offset_(l_meta_->value().value_offset(0)),
            compare_info_(l_meta_->key()),
            key_comparator_(compare_info_),
            key_size_(l_meta_->key().record_size()),

            l_store_resource_last_checkpoint_(l_store_resource_->get_checkpoint()),
            l_store_varlen_resource_last_checkpoint_(l_store_varlen_resource_->get_checkpoint()),
            r_store_resource_last_checkpoint_(r_store_resource_->get_checkpoint()),
            r_store_varlen_resource_last_checkpoint_(r_store_varlen_resource_->get_checkpoint())
    {}

    void consume_member(group_reader* reader,
            std::size_t& record_counter,
            std::size_t& key_counter,
            std::unique_ptr<data::iterable_record_store>& store) {

        while(reader->next_member()) {
            DVLOG(2) << *this << "   value : " << reader->get_member().get_value<double>(value_offset_);
            ++record_counter;
            total_val_ += reader->get_member().get_value<double>(value_offset_);

            auto rec = reader->get_member();
            store->append(rec);
        }
        key_counter++;
    }

    std::int64_t read_key(data::small_record_store const& key, meta::group_meta const& meta) {
        auto rec = key.ref();
        auto offset = meta.key().value_offset(0);
        return rec.get_value<std::int64_t>(offset);
    }

    void consume(std::function<void(std::int64_t, double, double)> const& consumer) {
        auto r_value_offset = r_meta_->value().value_offset(0);
        auto l_value_offset = l_meta_->value().value_offset(0);
        if(l_store_->empty()) {
            ++keys_right_only_;
            auto it = r_store_->begin();
            auto end = r_store_->end();
            DVLOG(2) << *this << " key : " << read_key(r_key_, *r_meta_);
            while(it != end) {
                consumer(read_key(r_key_, *r_meta_), -1.0, (*it).get_value<double>(r_value_offset));
                ++it;
                ++values_right_only_;
            }
        } else if (r_store_->empty()) {
            ++keys_left_only_;
            auto it = l_store_->begin();
            auto end = l_store_->end();
            DVLOG(2) << *this << " key : " << read_key(l_key_, *l_meta_);
            while(it != end) {
                consumer(read_key(l_key_, *l_meta_) ,(*it).get_value<double>(l_value_offset), -1.0);
                ++it;
                ++values_left_only_;
            }
        } else {
            ++keys_matched_;
            auto l_it = l_store_->begin();
            auto l_end = l_store_->end();
            auto r_end = r_store_->end();
            DVLOG(2) << *this << " key : " << read_key(l_key_, *l_meta_);
            while(l_it != l_end) {
                auto r_it = r_store_->begin();
                while(r_it != r_end) {
                    consumer(read_key(r_key_, *r_meta_) ,(*l_it).get_value<double>(l_value_offset), (*r_it).get_value<double>(r_value_offset));
                    ++r_it;
                    ++values_matched_;
                }
                ++l_it;
            }
        }
        l_store_->reset();
        r_store_->reset();
        l_store_resource_->deallocate_after(l_store_resource_last_checkpoint_);
        l_store_varlen_resource_->deallocate_after(l_store_varlen_resource_last_checkpoint_);
        r_store_resource_->deallocate_after(r_store_resource_last_checkpoint_);
        r_store_varlen_resource_->deallocate_after(r_store_varlen_resource_last_checkpoint_);

        l_store_resource_last_checkpoint_ = l_store_resource_->get_checkpoint();
        l_store_varlen_resource_last_checkpoint_ = l_store_varlen_resource_->get_checkpoint();
        r_store_resource_last_checkpoint_ = r_store_resource_->get_checkpoint();
        r_store_varlen_resource_last_checkpoint_ = r_store_varlen_resource_->get_checkpoint();
    }

    void next_left_key() {
        auto* l_reader = left_reader_.reader<executor::group_reader>();
        auto key = l_reader->get_group();
        l_key_.set(key);
    }
    void next_right_key() {
        auto* r_reader = right_reader_.reader<executor::group_reader>();
        auto key = r_reader->get_group();
        r_key_.set(key);
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
                case state::left_eof:
                    if(!r_reader->next_group()) {
                        right_eof = true;
                        s = state::end;
                        break;
                    }
                    next_right_key();
                    s = state::on_right_member;
                    break;
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
                    if (auto c = key_comparator_(l_key_.ref(),r_key_.ref()); c < 0) {
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
                    consume_member(l_reader, l_records_, l_keys_, l_store_);
                    if (r == read::both) {
                        s = state::on_right_member;
                    } else {
                        // send data
                        s = state::filled;
                    }
                    break;
                case state::on_right_member:
                    consume_member(r_reader, r_records_, r_keys_, r_store_);
                    // send data
                    s = state::filled;
                    break;
                case state::filled: {
                    consume([&](std::int64_t key, double x, double y) {
                        DVLOG(2) << *this << " key: " << key << " value1 : " << x << " value2 : " << y;
                        total_key_ += key;
                        total_val_ += x + y;
                    });
                    auto prev = r;
                    r = read::none;
                    if (prev == read::both) {
                        s = state::both_consumed;
                        break;
                    }
                    if (prev == read::none) {
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
                    if (prev == read::left) {
                        if(!l_reader->next_group()) {
                            left_eof = true;
                            s = state::on_right_member;
                            break;
                        }
                        next_left_key();
                        s = state::did_read_both_key;
                        break;
                    }
                    if (prev == read::right) {
                        if(!r_reader->next_group()) {
                            right_eof = true;
                            s = state::on_left_member;
                            break;
                        }
                        next_right_key();
                        s = state::did_read_both_key;
                        break;
                    }
                    std::abort();
                }
                case state::end:
                    break;
            }
        }
        l_reader->release();
        r_reader->release();
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
    std::unique_ptr<data::iterable_record_store> l_store_{};
    std::unique_ptr<data::iterable_record_store> r_store_{};
    executor::reader_container left_reader_{};
    executor::reader_container right_reader_{};
    data::small_record_store l_key_;
    data::small_record_store r_key_;

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

    memory::lifo_paged_memory_resource::checkpoint l_store_resource_last_checkpoint_{};
    memory::lifo_paged_memory_resource::checkpoint l_store_varlen_resource_last_checkpoint_{};
    memory::lifo_paged_memory_resource::checkpoint r_store_resource_last_checkpoint_{};
    memory::lifo_paged_memory_resource::checkpoint r_store_varlen_resource_last_checkpoint_{};
};

}



