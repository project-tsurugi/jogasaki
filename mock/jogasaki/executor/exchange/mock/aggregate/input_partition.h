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
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/record_writer.h>
#include <jogasaki/executor/exchange/mock/aggregate/shuffle_info.h>
#include <jogasaki/memory/page_pool.h>
#include <jogasaki/executor/hash.h>
#include <jogasaki/executor/comparator.h>
#include <jogasaki/utils/interference_size.h>

namespace jogasaki::executor::exchange::mock::aggregate {

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

    using key_pointer = void*;
    using value_pointer = void*;
    using bucket_type = tsl::detail_hopscotch_hash::hopscotch_bucket<std::pair<key_pointer, value_pointer>, 62, false>;
    using hash_table_allocator = boost::container::pmr::polymorphic_allocator<bucket_type>;
    using hash_table = tsl::hopscotch_map<key_pointer, value_pointer, hash, impl::key_eq, hash_table_allocator>;
    using hash_tables = std::vector<hash_table>;

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
        std::shared_ptr<shuffle_info> info,
        request_context* context,
        [[maybe_unused]] std::size_t initial_hash_table_size = default_initial_hash_table_size
    ) noexcept :
        resource_for_keys_(std::move(resource_for_keys)),
        resource_for_values_(std::move(resource_for_values)),
        resource_for_varlen_data_(std::move(resource_for_varlen_data)),
        resource_for_hash_tables_(std::move(resource_for_hash_tables)),
        info_(std::move(info)),
        context_(context),
        comparator_(info_->key_meta().get()),
        initial_hash_table_size_(initial_hash_table_size)
    {
        (void)context_;
    }

    /**
     * @brief write record to the input partition
     * @param record
     * @return whether flushing happens or not
     */
    bool write(accessor::record_ref record) {
        initialize_lazy();
        auto& table = tables_.back();
        auto key = info_->extract_key(record);
        auto value = info_->extract_value(record);
        if (auto it = table.find(key.data()); it != table.end()) {
            auto& aggregator = *info_->aggregator();
            aggregator(info_->value_meta().get(), accessor::record_ref(it->second, info_->value_meta()->record_size()), value);
        } else {
            table.emplace(keys_->append(key), values_->append(value));
            if (table.load_factor() > load_factor_bound) {
                flush();
                return true;
            }
            // TODO predict and avoid unexpected reallocation (e.g. all neighbors occupied) where memory allocator raises bad_alloc
        }
        return false;
    }

    /**
     * @brief finish current hash table
     * @details the current internal hash table is finalized and next write() will create new one.
     */
    void flush() noexcept {
        current_table_active_ = false;
    }

    /**
     * @brief returns the number of hash tables
     */
    [[nodiscard]] std::size_t tables_count() const noexcept {
        return tables_.size();
    }

    /**
     * @brief hash table read access interface with iterator
     * @details this object represents a reference to a hash table with an iterator on it
     */
    class iterable_hash_table {
    public:
        using iterator = hash_table::iterator;

        iterable_hash_table(hash_table& table,
            std::size_t key_size,
            std::size_t value_size
        ) noexcept : table_(std::addressof(table)), key_size_(key_size), value_size_(value_size), it_(table_->end()) {}

        /**
         * @brief proceed the internal iterator
         * @return whether the value on the forwarded iterator is available or not
         */
        [[nodiscard]] bool next() noexcept {
            if (it_ == table_->end()) {
                reset();
            } else {
                ++it_;
            }
            return it_ != table_->end();
        }

        /**
         * @brief reset the internal iterator position to beginning
         */
        void reset() noexcept {
            it_ = table_->begin();
        }

        /**
         * @brief access key on the internal iterator
         * @return key record
         */
        [[nodiscard]] accessor::record_ref key() const noexcept {
            return accessor::record_ref(it_->first, key_size_);
        }

        /**
         * @brief access value on the internal iterator
         * @return value record
         */
        [[nodiscard]] accessor::record_ref value() const noexcept {
            return accessor::record_ref(it_->second, value_size_);
        }

        /**
         * @brief find key/value on the hash table
         * @param key the record to find
         * @return iterator on the found record
         * @return end() when not found
         * @note this doesn't change the state of internal iterator. No const is specified as hopscotch doesn't either.
         */
        [[nodiscard]] iterator find(accessor::record_ref key) {
            return table_->find(key.data());
        }

        /**
         * @brief find key/value on the hash table
         * @param key the record to find
         * @param precalculated_hash precalulated hash corresponding to the key
         * @return iterator on the found record
         * @return end() when not found
         * @note this doesn't change the state of internal iterator. No const is specified as hopscotch doesn't either.
         */
        [[nodiscard]] iterator find(accessor::record_ref key, std::size_t precalculated_hash) {
            return table_->find(key.data(), precalculated_hash);
        }

        /**
         * @brief end iterator
         * @return iterator at the end of this hash map
         */
        [[nodiscard]] iterator end() const noexcept {
            return table_->end();
        }

        /**
         * @brief erase the element from the hash map
         * @param it iterator indicating the element for removal
         */
        void erase(iterator it) {
            table_->erase(it);
        }

        [[nodiscard]] std::size_t size() const noexcept {
            return table_->size();
        }

        [[nodiscard]] bool empty() const noexcept {
            return table_->empty();
        }

        /**
         * @brief calculate the hash for given key
         * @param key the record used to calculate the hash
         * @return the hash value
         */
        [[nodiscard]] std::size_t calculate_hash(accessor::record_ref key) const {
            return table_->hash_function()(key.data());
        }

    private:
        hash_table* table_{};
        std::size_t key_size_{};
        std::size_t value_size_{};
        iterator it_{};
    };

    /**
     * @brief check whether the hash table is empty or not
     * @param index the 0-origin index to specify the hash table. Must be less than the number of tables returned by tables_count().
     * @return true if the hash table is empty
     * @return false otherwise
     * @attention the behavior is undefined if given index is invalid
     */
    [[nodiscard]] bool empty(std::size_t index) const noexcept {
        return tables_[index].empty();
    }

    /**
     * @brief retrieve the hash table access object
     * @param index the 0-origin index to specify the hash table. Must be less than the number of tables returned by tables_count().
     * @return the object to access hash table with iterator
     */
    iterable_hash_table table_at(std::size_t index) {
        return iterable_hash_table(tables_[index],
            info_->key_meta()->record_size(),
            info_->value_meta()->record_size());
    }

private:
    std::unique_ptr<memory::paged_memory_resource> resource_for_keys_{};
    std::unique_ptr<memory::paged_memory_resource> resource_for_values_{};
    std::unique_ptr<memory::paged_memory_resource> resource_for_varlen_data_{};
    std::unique_ptr<memory::paged_memory_resource> resource_for_hash_tables_{};
    std::shared_ptr<shuffle_info> info_{};
    request_context* context_{};
    std::unique_ptr<data::record_store> keys_{};
    std::unique_ptr<data::record_store> values_{};
    hash_tables tables_{};
    comparator comparator_{};
    bool current_table_active_{false};
    std::size_t initial_hash_table_size_{};

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
        if(! current_table_active_) {
            tables_.emplace_back(initial_hash_table_size_,
                hash{info_->key_meta().get()},
                impl::key_eq{comparator_, info_->key_meta()->record_size()},
                hash_table_allocator{resource_for_hash_tables_.get()}
                );
            current_table_active_ = true;
        }
    }
};

}
