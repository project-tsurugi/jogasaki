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

#include <takatori/util/universal_extractor.h>
#include <takatori/util/reference_list_view.h>

#include <jogasaki/utils/round.h>
#include <jogasaki/request_context.h>
#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/data/record_store.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/record_writer.h>
#include <jogasaki/executor/exchange/aggregate/shuffle_info.h>
#include <jogasaki/memory/page_pool.h>
#include <jogasaki/executor/hash.h>
#include <jogasaki/executor/comparator.h>

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
 * @details This object represents group exchange input data after partition.
 * This object is transferred between sinks and sources when transfer is instructed to the exchange.
 * No limit to the number of records stored in this object.
 * After populating input data (by write() and flush()), this object provides iterators to the internal pointer tables
 * (each of which needs to fit page size defined by memory allocator, e.g. 2MB for huge page)
 * which contain sorted pointers.
 */
class input_partition {
public:

    using key_pointer = void*;
    using value_pointer = void*;
    using bucket_type = tsl::detail_hopscotch_hash::hopscotch_bucket<std::pair<key_pointer, value_pointer>, 62, false>;
    using hash_table_allocator = boost::container::pmr::polymorphic_allocator<bucket_type>;
    using hash_table = tsl::hopscotch_map<key_pointer, value_pointer, hash, impl::key_eq, hash_table_allocator>;
    using hash_tables = std::vector<hash_table>;
    using iterator = hash_tables::iterator;

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
        std::shared_ptr<request_context> context,
        [[maybe_unused]] std::size_t initial_hash_table_size = default_initial_hash_table_size
    ) :
        resource_for_keys_(std::move(resource_for_keys)),
        resource_for_values_(std::move(resource_for_values)),
        resource_for_varlen_data_(std::move(resource_for_varlen_data)),
        resource_for_hash_tables_(std::move(resource_for_hash_tables)),
        info_(std::move(info)),
        context_(std::move(context)),
        comparator_(info_->key_meta().get()),
        initial_hash_table_size_(initial_hash_table_size),
        key_size_(info_->key_meta()->record_size()),
        value_size_(info_->value_meta()->record_size())
    {}

    /**
     * @brief write record to the input partition
     * @param record
     * @return whether flushing pointer table happens or not
     */
    bool write(accessor::record_ref record) {
        initialize_lazy();
        auto& table = maps_.back();
        auto key = info_->extract_key(record);
        auto value = info_->extract_value(record);
        if (auto it = table.find(key.data()); it != table.end()) {
            auto& aggregator = *info_->aggregator();
            aggregator(info_->value_meta().get(), accessor::record_ref(it->second, value_size_), value);
        } else {
            table.emplace(keys_->append(key), values_->append(value));
            if (table.load_factor() > load_factor_bound) {  // TODO avoid reallocation completely
                flush();
                return true;
            }
        }
        return false;
    }

    /**
     * @brief finish current pointer table
     * @details the current internal pointer table is finalized and next write() will create one.
     */
    void flush() {
        if(!current_table_active_) return;
        current_table_active_ = false;
    }

    /**
     * @brief returns the number of pointer tables
     */
    [[nodiscard]] std::size_t tables_count() const noexcept {
        return maps_.size();
    }

    class iteratable_map {
    public:
        using iterator = input_partition::hash_table::iterator;

        iteratable_map(input_partition* owner, input_partition::hash_table& table) : owner_(owner), table_(std::addressof(table)), it_(table_->begin()) {}

        bool next() noexcept {
            if (! initialized_) {
                reset();
            } else {
                ++it_;
            }
            return it_ != table_->end();
        }
        void reset() noexcept {
            it_ = table_->begin();
            initialized_ = true;
        }
        [[nodiscard]] accessor::record_ref key() const noexcept {
            return accessor::record_ref(it_->first, owner_->key_size_);
        }

        accessor::record_ref value() noexcept {
            return accessor::record_ref(it_->second, owner_->value_size_);
        }

        iterator find(accessor::record_ref key) {
            return table_->find(key.data());
        }

        [[nodiscard]] iterator end() const noexcept {
            return table_->end();
        }

        void erase(iterator it) {
            table_->erase(it);
        }

        [[nodiscard]] std::size_t size() const noexcept {
            return table_->size();
        }

        [[nodiscard]] bool empty() const noexcept {
            return table_->empty();
        }
    private:
        input_partition* owner_{};
        input_partition::hash_table* table_;
        input_partition::hash_table::iterator it_{};
        bool initialized_{false};
    };

    iteratable_map maps(std::size_t index) {
        return iteratable_map(this, maps_[index]);
    }

private:
    std::unique_ptr<memory::paged_memory_resource> resource_for_keys_{};
    std::unique_ptr<memory::paged_memory_resource> resource_for_values_{};
    std::unique_ptr<memory::paged_memory_resource> resource_for_varlen_data_{};
    std::unique_ptr<memory::paged_memory_resource> resource_for_hash_tables_{};
    std::shared_ptr<shuffle_info> info_{};
    std::shared_ptr<request_context> context_{std::make_shared<request_context>()};
    std::unique_ptr<data::record_store> keys_{};
    std::unique_ptr<data::record_store> values_{};
    hash_tables maps_{};
    comparator comparator_{};
    bool current_table_active_{false};
    std::size_t initial_hash_table_size_{};
    std::size_t key_size_{};
    std::size_t value_size_{};

    void initialize_lazy() {
        if (!keys_) {
            keys_ = std::make_unique<data::record_store>(
                    resource_for_keys_.get(),
                    resource_for_varlen_data_.get(),
                    info_->key_meta());
        }
        if (!values_) {
            values_ = std::make_unique<data::record_store>(
                resource_for_values_.get(),
                resource_for_varlen_data_.get(),
                info_->value_meta());
        }
        if(!current_table_active_) {
            maps_.emplace_back(initial_hash_table_size_,
                hash{info_->key_meta().get()},
                impl::key_eq{comparator_, key_size_},
                hash_table_allocator{resource_for_hash_tables_.get()}
                );
            current_table_active_ = true;
        }
    }
};

}
