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
#include <queue>
#include <glog/logging.h>

#include <jogasaki/model/task.h>
#include <jogasaki/model/step.h>
#include <jogasaki/meta/group_meta.h>
#include <jogasaki/executor/common/task.h>
#include <jogasaki/executor/group_reader.h>
#include <jogasaki/executor/reader_container.h>
#include <jogasaki/data/iterable_record_store.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/executor/comparator.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/data/small_record_store.h>

#include "../iterator_pair.h"

namespace jogasaki::executor::process::impl::ops {

namespace impl {

using checkpoint = memory::lifo_paged_memory_resource::checkpoint;

class cogroup_record_store {
public:
    using iterator = data::iterable_record_store::iterator;

    explicit cogroup_record_store(
            std::unique_ptr<data::iterable_record_store> store,
            std::unique_ptr<memory::lifo_paged_memory_resource> resource = {},
            std::unique_ptr<memory::lifo_paged_memory_resource> varlen_resource = {}
    ) :
            store_(std::move(store)),
            resource_(std::move(resource)),
            varlen_resource_(std::move(varlen_resource)),
            resource_last_checkpoint_(resource_ ? resource_->get_checkpoint() : checkpoint{}),
            varlen_resource_last_checkpoint_(varlen_resource_ ? varlen_resource_->get_checkpoint() : checkpoint{})
    {}

    [[nodiscard]] data::iterable_record_store& store() const noexcept {
        return *store_;
    }

    iterator begin() {
        return store_->begin();
    }

    iterator end() {
        return store_->end();
    }

    void reset() {
        store_->reset();
        if (resource_) {
            resource_->deallocate_after(resource_last_checkpoint_);
            resource_last_checkpoint_ = resource_->get_checkpoint();
        }
        if (varlen_resource_) {
            varlen_resource_->deallocate_after(varlen_resource_last_checkpoint_);
            varlen_resource_last_checkpoint_ = varlen_resource_->get_checkpoint();
        }
    }

private:
    std::unique_ptr<data::iterable_record_store> store_{};
    std::unique_ptr<memory::lifo_paged_memory_resource> resource_{};
    std::unique_ptr<memory::lifo_paged_memory_resource> varlen_resource_{};
    memory::lifo_paged_memory_resource::checkpoint resource_last_checkpoint_{};
    memory::lifo_paged_memory_resource::checkpoint varlen_resource_last_checkpoint_{};
};

/**
 * @brief responsible for reading from reader and filling the record store
 */
class cogroup_input {
public:
    cogroup_input(
            executor::group_reader& reader,
            std::unique_ptr<cogroup_record_store> store,
            std::shared_ptr<meta::group_meta> meta
    ) :
            reader_(std::addressof(reader)),
            store_(std::move(store)),
            meta_(std::move(meta)),
            key_size_(meta_->key().record_size()),
            key_(meta_->key_shared()),
            key_comparator_(meta_->key_shared().get())
    {}

    [[nodiscard]] accessor::record_ref key_record() const noexcept {
        BOOST_ASSERT(key_filled_);  //NOLINT
        return key_.ref();
    }

    std::shared_ptr<meta::group_meta> const& meta() {
        return meta_;
    }

    [[nodiscard]] bool eof() const noexcept {
        return reader_eof_;
    }

    [[nodiscard]] bool filled() const noexcept {
        return values_filled_;
    }

    [[nodiscard]] bool key_filled() const noexcept {
        return key_filled_;
    }

    iterator begin() {
        return store_->begin();
    }

    iterator end() {
        return store_->end();
    }

    bool next() {
        if(!reader_->next_group()) {
            key_filled_ = false;
            reader_eof_ = true;
            return false;
        }
        key_.set(reader_->get_group());
        key_filled_ = true;
        return true;
    }

    void fill() noexcept {
        while(reader_->next_member()) {
            auto rec = reader_->get_member();
            store_->store().append(rec);
        }
        values_filled_ = true;
    }

    void reset_store() {
        if (values_filled_) {
            store_->reset();
            values_filled_ = false;
        }
    }

private:
    executor::group_reader* reader_{};
    std::unique_ptr<cogroup_record_store> store_;
    std::shared_ptr<meta::group_meta> meta_{};
    std::size_t key_size_ = 0;
    data::small_record_store key_; // shallow copy of key (varlen body is held by reader)
    comparator key_comparator_{};
    bool reader_eof_{false};
    bool values_filled_{false};
    bool key_filled_{false};
};

/**
 * @brief cogroup input comparator
 * @details comparator to compare cogroup_input with its current key value
 * like std::greater, this comparator returns true when x > y, where x and y are 1st and 2nd args.
 * This is intended to be used with std::priority_queue, which positions the greatest at the top.
 */
class cogroup_input_comparator {
public:
    using input_index = std::size_t;
    /**
     * @brief construct new object
     * @attention key_meta is kept and used by the comparator. The caller must ensure it outlives this object.
     */
    explicit cogroup_input_comparator(std::vector<cogroup_input>* inputs, meta::record_meta const* key_meta) :
            inputs_(inputs),
            key_comparator_(key_meta) {}

    bool operator()(input_index const& x, input_index const& y) {
        auto& l = inputs_->operator[](x);
        auto& r = inputs_->operator[](y);
        return key_comparator_(l.key_record(), r.key_record()) > 0;
    }

private:
    std::vector<cogroup_input>* inputs_{};
    comparator key_comparator_{};
};

} // namespace impl

class cogroup {
public:
    using input_index = std::size_t;

    using queue_type = std::priority_queue<input_index, std::vector<input_index>, impl::cogroup_input_comparator>;

    using consumer_type = std::function<void(accessor::record_ref, std::vector<iterator_pair>&)>;

    cogroup(
            std::vector<executor::group_reader*> readers,
            std::vector<std::shared_ptr<meta::group_meta>> groups_meta
    ) :
            readers_(std::move(readers)),
            groups_meta_(std::move(groups_meta)),
            queue_(impl::cogroup_input_comparator(&inputs_, groups_meta_[0]->key_shared().get())),
            key_size_(groups_meta_[0]->key().record_size()),
            key_comparator_(&groups_meta_[0]->key()),
            key_buf_(groups_meta_[0]->key_shared())
            // assuming key meta are common to all inputs TODO add assert
    {
        BOOST_ASSERT(readers_.size() == groups_meta_.size());  //NOLINT
        for(std::size_t idx = 0, n = readers_.size(); idx < n; ++idx) {
            auto resource = std::make_unique<memory::lifo_paged_memory_resource>(&global::page_pool());
            auto varlen_resource = std::make_unique<memory::lifo_paged_memory_resource>(&global::page_pool());
            auto meta = groups_meta_[idx];
            auto store = std::make_unique<data::iterable_record_store>(
                    resource.get(),
                    varlen_resource.get(),
                    meta->value_shared()
            );
            inputs_.emplace_back(
                    *readers_[idx],
                    std::make_unique<impl::cogroup_record_store>(
                            std::move(store),
                            std::move(resource),
                            std::move(varlen_resource)),
                    std::move(meta)
            );
        }
    }

    void operator()(consumer_type& consumer) {
        enum class state {
            init,
            keys_filled,
            values_filled,
            end,
        };

        state s{state::init};
        while(s != state::end) {
            switch(s) {
                case state::init:
                    for(input_index idx = 0, n = inputs_.size(); idx < n; ++idx) {
                        auto& in = inputs_[idx];
                        if(in.next()) {
                            queue_.emplace(idx);
                        } else {
                            BOOST_ASSERT(in.eof());  //NOLINT
                        }
                    }
                    s = state::keys_filled;
                    break;
                case state::keys_filled: {
                    if (queue_.empty()) {
                        s = state::end;
                        break;
                    }
                    auto idx = queue_.top();
                    queue_.pop();
                    inputs_[idx].fill();
                    key_buf_.set(inputs_[idx].key_record());
                    auto key = key_buf_.ref();
                    if(inputs_[idx].next()) {
                        queue_.emplace(idx);
                    }
                    while(!queue_.empty()) {
                        auto idx2 = queue_.top();
                        if (key_comparator_(inputs_[idx2].key_record(), key) != 0) {
                            break;
                        }
                        queue_.pop();
                        inputs_[idx2].fill();
                        if(inputs_[idx2].next()) {
                            queue_.emplace(idx2);
                        }
                    }
                    s = state::values_filled;
                    break;
                }
                case state::values_filled:
                    consume(consumer);
                    s = state::keys_filled;
                    break;
                case state::end:
                    break;
            }
        }
        for(auto* r : readers_) {
            r->release();
        }
    }

private:
    std::vector<executor::group_reader*> readers_{};
    std::vector<std::shared_ptr<meta::group_meta>> groups_meta_{};
    std::vector<impl::cogroup_input> inputs_{};
    queue_type queue_;
    std::size_t key_size_{};
    comparator key_comparator_{};
    data::small_record_store key_buf_; // shallow copy of key (varlen body is held by reader)

    void consume(consumer_type& consumer) {
        auto key = key_buf_.ref();
        std::vector<iterator_pair> iterators{};
        auto inputs = inputs_.size();
        for(input_index i = 0; i < inputs; ++i) {
            iterators.emplace_back(inputs_[i].begin(), inputs_[i].end());
        }
        consumer(key, iterators);
        for(input_index i = 0; i < inputs; ++i) {
            inputs_[i].reset_store();
        }
    }

};

}
