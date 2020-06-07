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
#include <jogasaki/data/iteratable_record_store.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/utils/aligned_unique_ptr.h>
#include <jogasaki/executor/comparator.h>
#include <jogasaki/executor/global.h>

#include "impl/iterator_pair.h"

namespace jogasaki::executor::process {

using namespace jogasaki::executor;

namespace impl {

using checkpoint = memory::lifo_paged_memory_resource::checkpoint;

class cogroup_record_store {
public:
    using iterator = data::iteratable_record_store::iterator;

    cogroup_record_store(
            std::unique_ptr<data::iteratable_record_store> store,
            std::unique_ptr<memory::lifo_paged_memory_resource> resource = {},
            std::unique_ptr<memory::lifo_paged_memory_resource> varlen_resource = {}
    ) :
            store_(std::move(store)),
            resource_(std::move(resource)),
            varlen_resource_(std::move(varlen_resource)),
            resource_last_checkpoint_(resource_ ? resource_->get_checkpoint() : checkpoint{}),
            varlen_resource_last_checkpoint_(varlen_resource_ ? varlen_resource_->get_checkpoint() : checkpoint{})
    {}

    [[nodiscard]] data::iteratable_record_store& store() const noexcept {
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
    std::unique_ptr<data::iteratable_record_store> store_{};
    std::unique_ptr<memory::lifo_paged_memory_resource> resource_{};
    std::unique_ptr<memory::lifo_paged_memory_resource> varlen_resource_{};
    memory::lifo_paged_memory_resource::checkpoint resource_last_checkpoint_{};
    memory::lifo_paged_memory_resource::checkpoint varlen_resource_last_checkpoint_{};
};

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
            key_(utils::make_aligned_array<char>(meta_->key().record_alignment(), key_size_)),
            key_comparator_(meta_->key_shared().get())
    {}

    [[nodiscard]] executor::group_reader& reader() const noexcept {
        return *reader_;
    }

    [[nodiscard]] cogroup_record_store& store() const noexcept {
        return *store_;
    }

    [[nodiscard]] utils::aligned_array<char> const& key() const noexcept {
        return key_;
    }

    [[nodiscard]] accessor::record_ref key_record() const noexcept {
        return accessor::record_ref(key_.get(), key_size_);
    }

    [[nodiscard]] std::size_t key_size() const noexcept {
        return key_size_;
    }

    std::shared_ptr<meta::group_meta> const& meta() {
        return meta_;
    }

    [[nodiscard]] bool eof() const noexcept {
        return reader_eof_;
    }

    void eof(bool arg = true) noexcept {
        reader_eof_ = arg;
    }

    [[nodiscard]] bool filled() const noexcept {
        return filled_;
    }

private:
    executor::group_reader* reader_{};
    std::unique_ptr<cogroup_record_store> store_;
    std::shared_ptr<meta::group_meta> meta_{};
    std::size_t key_size_ = 0;
    utils::aligned_array<char> key_;
    comparator key_comparator_{};
    bool reader_eof_{false};
    bool filled_{false};
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

    using consumer_type = std::function<void(accessor::record_ref, std::vector<impl::iterator_pair>&)>;

    cogroup(
            std::vector<executor::group_reader*> readers,
            std::vector<std::shared_ptr<meta::group_meta>> groups_meta
    ) :
            readers_(std::move(readers)),
            groups_meta_(std::move(groups_meta)),
            queue_(impl::cogroup_input_comparator(&inputs_, groups_meta_[0]->key_shared().get()))
            // assuming key meta are common to all inputs TODO add assert
    {
        assert(readers_.size() == groups_meta_.size());
        for(std::size_t idx = 0, n = readers_.size(); idx < n; ++idx) {
            auto resource = std::make_unique<memory::lifo_paged_memory_resource>(&global::page_pool());
            auto varlen_resource = std::make_unique<memory::lifo_paged_memory_resource>(&global::page_pool());
            auto meta = groups_meta_[idx];
            auto store = std::make_unique<data::iteratable_record_store>(
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

    void consume_member(group_reader* reader,
            std::unique_ptr<data::iteratable_record_store>& store) {
        while(reader->next_member()) {
            auto rec = reader->get_member();
            store->append(rec);
        }
    }

    void consume(consumer_type& consumer) {
        accessor::record_ref key{};
        std::vector<impl::iterator_pair> iterators{};
        auto inputs = inputs_.size();
        for(input_index i = 0; i < inputs; ++i) {
            if(!key && inputs_[i].filled()) {
                key = inputs_[i].key_record();
            }
            iterators.emplace_back(inputs_[i].store().begin(), inputs_[i].store().end());
        }
        if (!key) {
            takatori::util::fail();
        }
        consumer(key, iterators);

        for(input_index i = 0; i < inputs; ++i) {
            inputs_[i].store().reset();
        }
    }

    void next_key(input_index idx) {
        auto& reader = inputs_[idx].reader();
        auto key = reader.get_group();
        std::memcpy(inputs_[idx].key().get(), key.data(), key.size());
    }

    void operator()(consumer_type& consumer) {
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
        state s{state::init};
        read r{read::none};
        while(s != state::end) {
            switch(s) {
                case state::init:
                case state::both_consumed:
                default:
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
    std::priority_queue<input_index, std::vector<input_index>, impl::cogroup_input_comparator> queue_;
};

}
