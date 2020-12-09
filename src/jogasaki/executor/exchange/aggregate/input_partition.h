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

#include <tsl/hopscotch_map.h>
#include <boost/container/pmr/polymorphic_allocator.hpp>

#include <jogasaki/utils/round.h>
#include <jogasaki/request_context.h>
#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/data/record_store.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/record_writer.h>
#include <jogasaki/executor/exchange/aggregate/aggregate_info.h>
#include <jogasaki/memory/page_pool.h>
#include <jogasaki/executor/hash.h>
#include <jogasaki/executor/comparator.h>
#include <jogasaki/utils/interference_size.h>
#include <jogasaki/utils/copy_field_data.h>
#include <jogasaki/executor/exchange/shuffle/pointer_table.h>

namespace jogasaki::executor::exchange::aggregate {

namespace impl {
    class key_eq {
    public:
        using key_pointer = void*;
        key_eq(comparator& comp, std::size_t key_size) : comp_(std::addressof(comp)), key_size_(key_size) {}

        bool operator()(key_pointer const& a, key_pointer const& b) const noexcept {
            return comp_->operator()(accessor::record_ref(a, key_size_), accessor::record_ref(b, key_size_)) == 0;
        }
    private:
        comparator* comp_{};
        std::size_t key_size_{};
    };
}

/**
 * @brief partitioned input data handled in upper phase in shuffle
 * @details This object represents aggregate exchange input data after partition.
 * This object is transferred between sinks and sources when transfer is instructed to the exchange.
 * No limit to the number of records stored in this object.
 * After populating input data (by write() and flush()), this object provides iterable hash tables
 * (each of which needs to fit page size defined by memory allocator, e.g. 2MB for huge page)
 * which contain (locally pre-aggregated) key-value pairs.
 */
class cache_align input_partition {
public:

    using pointer_table_type = shuffle::pointer_table;
    using pointer_tables_type = std::vector<pointer_table_type>;
    using iterator = pointer_tables_type::iterator;
    using table_iterator = pointer_table_type::iterator;
    constexpr static std::size_t ptr_table_size = memory::page_size/sizeof(void*);

    using key_pointer = void*;
    using value_pointer = void*;
    using bucket_type = tsl::detail_hopscotch_hash::hopscotch_bucket<std::pair<key_pointer, value_pointer>, 62, false>;
    using hash_table_allocator = boost::container::pmr::polymorphic_allocator<bucket_type>;
    using hash_table = tsl::hopscotch_map<key_pointer, value_pointer, hash, impl::key_eq, hash_table_allocator>;

    // hopscotch default power_of_two_growth_policy forces the # of buckets to be power of two, so round down here to avoid going over allocator limit
    constexpr static std::size_t default_initial_hash_table_size = utils::round_down_to_power_of_two(memory::page_size / sizeof(bucket_type));

    static_assert(sizeof(hash_table::value_type) == 16);  // two pointers
    static_assert(alignof(hash_table::value_type) == 8);
    static_assert(sizeof(bucket_type::neighborhood_bitmap) == 8);
    static_assert(alignof(bucket_type::neighborhood_bitmap) == 8);

    ///@brief upper bound of load factor to flush()
    constexpr static float load_factor_bound = 0.7;

    input_partition() = default;

    /**
     * @brief create new instance
     * @param resource
     * @param info
     */
    input_partition(
        std::unique_ptr<memory::paged_memory_resource> resource_for_keys,
        std::unique_ptr<memory::paged_memory_resource> resource_for_values,
        std::unique_ptr<memory::paged_memory_resource> resource_for_varlen_data,
        std::unique_ptr<memory::paged_memory_resource> resource_for_hash_tables,
        std::unique_ptr<memory::paged_memory_resource> resource_for_ptr_tables,
        std::shared_ptr<aggregate_info> info,
        [[maybe_unused]] std::size_t initial_hash_table_size = default_initial_hash_table_size,
        [[maybe_unused]] std::size_t pointer_table_size = ptr_table_size
    ) noexcept :
        resource_for_keys_(std::move(resource_for_keys)),
        resource_for_values_(std::move(resource_for_values)),
        resource_for_varlen_data_(std::move(resource_for_varlen_data)),
        resource_for_hash_tables_(std::move(resource_for_hash_tables)),
        resource_for_ptr_tables_(std::move(resource_for_ptr_tables)),
        info_(std::move(info)),
        comparator_(info_->key_meta().get()),
        initial_hash_table_size_(initial_hash_table_size),
        max_pointers_(pointer_table_size),
        key_buf_(info_->key_meta())
    {}

    /**
     * @brief write record to the input partition
     * @param record
     * @return whether flushing happens or not
     */
    bool write(accessor::record_ref record) {
        initialize_lazy();
        auto& key_meta = info_->key_meta();
        auto key_indices = info_->key_indices();
        auto key_buf = key_buf_.ref();
        auto& record_meta = info_->record_meta();
        for(std::size_t i=0, n = key_indices.size(); i < n; ++i) {
            auto input_record_field = key_indices[i];
            utils::copy_nullable_field(
                record_meta->at(input_record_field),
                key_buf,
                key_meta->value_offset(i),
                key_meta->nullity_offset(i),
                record,
                record_meta->value_offset(input_record_field),
                record_meta->nullity_offset(input_record_field),
                keys_->varlen_resource()
            );
        }
        auto& value_meta = info_->value_meta();
        accessor::record_ref value{};
        bool initial = false;
        if (auto it = hash_table_->find(key_buf.data()); it != hash_table_->end()) {
            value = accessor::record_ref(it->second, info_->value_meta()->record_size());
        } else {
            initial = true;
            value = accessor::record_ref{values_->allocate_record(), value_meta->record_size()};
            accessor::record_ref key{keys_->allocate_record(), key_meta->record_size()};
            keys_->copier()(key, key_buf);
            key.set_value<void*>(info_->key_meta()->value_offset(info_->key_meta()->field_count()-1), value.data());
            hash_table_->emplace(keys_->append(key), values_->append(value));
            if(! current_table_active_) {
                pointer_tables_.emplace_back(resource_for_ptr_tables_.get(), max_pointers_);
                current_table_active_ = true;
            }
            auto& table = pointer_tables_.back();
            table.emplace_back(key.data());
        }
        for(std::size_t i=0, n = info_->value_specs().size(); i < n; ++i) {
            auto& vspec = info_->value_specs()[i];
            auto& aggregator = vspec.aggregator();
            aggregator(value, value_meta->value_offset(i), initial, record, info_->aggregators_args(i));
        }
        if (hash_table_->load_factor() > load_factor_bound) {
            flush();
            return true;
        }
        // TODO predict and avoid unexpected reallocation (e.g. all neighbors occupied) where memory allocator raises bad_alloc
        return false;
    }

    /**
     * @brief finish current hash table
     * @details the current internal hash table is finalized and next write() will create new one.
     */
    void flush() {
        if(! current_table_active_) return;
        current_table_active_ = false;
        auto sz = info_->record_meta()->record_size();
        auto& table = pointer_tables_.back();
        std::sort(table.begin(), table.end(), [&](auto const&x, auto const& y){
            return comparator_(
                accessor::record_ref(x, sz),
                accessor::record_ref(y, sz)) < 0;
        });
    }

    /**
     * @brief beginning iterator for pointer tables
     */
    [[nodiscard]] iterator begin() {
        return pointer_tables_.begin();
    }

    /**
     * @brief ending iterator for pointer tables
     */
    [[nodiscard]] iterator end() {
        return pointer_tables_.end();
    }

    /**
     * @brief returns the number of pointer tables
     */
    [[nodiscard]] std::size_t tables_count() const noexcept {
        return pointer_tables_.size();
    }

    /**
     * @brief check whether the hash table is empty or not
     * @param index the 0-origin index to specify the hash table. Must be less than the number of tables returned by tables_count().
     * @return true if the hash table is empty
     * @return false otherwise
     * @attention the behavior is undefined if given index is invalid
     */
    [[nodiscard]] bool empty(std::size_t index) const noexcept {
        return pointer_tables_[index].empty();
    }

    /**
     * @brief retrieve the hash table access object
     * @param index the 0-origin index to specify the hash table. Must be less than the number of tables returned by tables_count().
     * @return the object to access hash table with iterator
     */
//    pointer_table_type table_at(std::size_t index) {
//        return iterable_hash_table(*tables_[index],
//            info_->key_meta()->record_size(),
//            info_->value_meta()->record_size());
//    }

    void release_hashtable() noexcept {
        hash_table_.reset();
    }
private:
    std::unique_ptr<memory::paged_memory_resource> resource_for_keys_{};
    std::unique_ptr<memory::paged_memory_resource> resource_for_values_{};
    std::unique_ptr<memory::paged_memory_resource> resource_for_varlen_data_{};
    std::unique_ptr<memory::paged_memory_resource> resource_for_hash_tables_{};
    std::unique_ptr<memory::paged_memory_resource> resource_for_ptr_tables_{};
    std::shared_ptr<aggregate_info> info_{};
    std::unique_ptr<data::record_store> keys_{};
    std::unique_ptr<data::record_store> values_{};
    std::unique_ptr<hash_table> hash_table_{};
    pointer_tables_type pointer_tables_{};
    comparator comparator_{};
    bool current_table_active_{false};
    std::size_t initial_hash_table_size_{};
    std::size_t max_pointers_{};
    data::small_record_store key_buf_;

    void initialize_lazy() {
        if (! keys_) {
            keys_ = std::make_unique<data::record_store>(
                    resource_for_keys_.get(),
                    resource_for_varlen_data_.get(),
                    info_->key_meta());
        }
        if (! values_) {
            values_ = std::make_unique<data::record_store>(
                resource_for_values_.get(),
                resource_for_varlen_data_.get(),
                info_->value_meta());
        }
        if(! hash_table_) {
            hash_table_ = std::make_unique<hash_table>(initial_hash_table_size_,
                hash{info_->key_meta().get()},
                impl::key_eq{comparator_, info_->key_meta()->record_size()},
                hash_table_allocator{resource_for_hash_tables_.get()}
            );
        }
    }
};

}
