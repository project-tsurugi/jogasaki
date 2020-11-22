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

#include <jogasaki/data/small_record_store.h>
#include <jogasaki/executor/group_reader.h>
#include "context_base.h"

namespace jogasaki::executor::process::impl::ops {

namespace details {

using checkpoint = memory::lifo_paged_memory_resource::checkpoint;

/**
 * @brief responsible for reading from reader and filling the record store
 */
class group_input {
public:
    using iterator = data::iterable_record_store::iterator;

    group_input(
        executor::group_reader& reader,
        std::unique_ptr<data::iterable_record_store> store,
        memory::lifo_paged_memory_resource* resource,
        memory::lifo_paged_memory_resource* varlen_resource,
        maybe_shared_ptr<meta::group_meta> meta
    ) :
        reader_(std::addressof(reader)),

        store_(std::move(store)),

        resource_(resource),
        varlen_resource_(varlen_resource),
        resource_last_checkpoint_(resource_ ? resource_->get_checkpoint() : checkpoint{}),
        varlen_resource_last_checkpoint_(varlen_resource_ ? varlen_resource_->get_checkpoint() : checkpoint{}),

        meta_(std::move(meta)),
        key_size_(meta_->key().record_size()),
        key_(meta_->key_shared())
    {}

    [[nodiscard]] accessor::record_ref key_record() const noexcept {
        BOOST_ASSERT(key_filled_);  //NOLINT
        return key_.ref();
    }

    [[nodiscard]] maybe_shared_ptr<meta::group_meta> const& meta() {
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

    [[nodiscard]] iterator begin() {
        return store_->begin();
    }

    [[nodiscard]] iterator end() {
        return store_->end();
    }

    [[nodiscard]] bool next() {
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
            store_->append(rec);
        }
        values_filled_ = true;
    }

    void reset_store() {
        if (values_filled_) {
            store_->reset();
            values_filled_ = false;
        }
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
    executor::group_reader* reader_{};

    std::unique_ptr<data::iterable_record_store> store_{};
    memory::lifo_paged_memory_resource* resource_{};
    memory::lifo_paged_memory_resource* varlen_resource_{};
    memory::lifo_paged_memory_resource::checkpoint resource_last_checkpoint_{};
    memory::lifo_paged_memory_resource::checkpoint varlen_resource_last_checkpoint_{};

    maybe_shared_ptr<meta::group_meta> meta_{};
    std::size_t key_size_ = 0;
    data::small_record_store key_; // shallow copy of key (varlen body is held by reader)
    bool reader_eof_{false};
    bool values_filled_{false};
    bool key_filled_{false};
};

/**
 * @brief group input comparator
 * @details comparator to compare group_input with its current key value
 * like std::greater, this comparator returns true when x > y, where x and y are 1st and 2nd args.
 * This is intended to be used with std::priority_queue, which positions the greatest at the top.
 */
class group_input_comparator {
public:
    using input_index = std::size_t;

    /**
     * @brief create undefined object
     */
    group_input_comparator() = default;

    /**
     * @brief construct new object
     * @attention key_meta is kept and used by the comparator. The caller must ensure it outlives this object.
     */
    group_input_comparator(std::vector<group_input>* inputs, meta::record_meta const* key_meta) :
        inputs_(inputs),
        key_comparator_(key_meta)
    {}

    [[nodiscard]] bool operator()(input_index const& x, input_index const& y) {
        auto& l = inputs_->operator[](x);
        auto& r = inputs_->operator[](y);
        return key_comparator_(l.key_record(), r.key_record()) > 0;
    }

private:
    std::vector<group_input>* inputs_{};
    comparator key_comparator_{};
};

} // namespace details

/**
 * @brief take_group context
 */
class take_cogroup_context : public context_base {
public:
    friend class take_cogroup;
    using input_index = std::size_t;
    using queue_type = std::priority_queue<input_index, std::vector<input_index>, details::group_input_comparator>;
    /**
     * @brief create empty object
     */
    take_cogroup_context() = default;

    /**
     * @brief create new object
     */
    take_cogroup_context(
        class abstract::task_context* ctx,
        block_scope& variables,
        maybe_shared_ptr<meta::record_meta> key_meta,
        memory_resource* resource,
        memory_resource* varlen_resource
    ) :
        context_base(ctx, variables, resource, varlen_resource),
        key_buf_(std::move(key_meta))
    {}

    [[nodiscard]] operator_kind kind() const noexcept override {
        return operator_kind::take_cogroup;
    }

    void release() override {
        for(auto* r : readers_) {
            if(r) {
                r->release();
            }
        }
    }

private:
    std::vector<executor::group_reader*> readers_{};
    std::vector<details::group_input> inputs_{};
    data::small_record_store key_buf_{}; // shallow copy of key (varlen body is held by reader)
    queue_type queue_{};
};

}


