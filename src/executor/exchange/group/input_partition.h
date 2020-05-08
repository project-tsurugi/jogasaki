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

#include <boost/container/pmr/vector.hpp>
#include <takatori/util/universal_extractor.h>
#include <takatori/util/reference_list_view.h>

#include <accessor/record_ref.h>
#include <data/variable_legnth_data_region.h>
#include <executor/global.h>
#include <executor/record_writer.h>
#include <executor/exchange/group/shuffle_info.h>
#include <memory/page_pool.h>

namespace jogasaki::executor::exchange::group {

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
    using pointer_table_type = boost::container::pmr::vector<void*>;
    using pointer_tables_type = std::vector<pointer_table_type>;
    using iterator = pointer_tables_type::iterator;
    using table_iterator = pointer_table_type::iterator;
    constexpr static std::size_t ptr_table_size = memory::page_size/sizeof(void*);

    input_partition() = default;
    ~input_partition() = default;
    input_partition(input_partition&& other) noexcept = delete;
    input_partition& operator=(input_partition&& other) noexcept = delete;

    /**
     * @brief create new instance
     * @param resource
     * @param info
     */
    input_partition(
            std::unique_ptr<memory::paged_memory_resource> resource_for_records,
            std::unique_ptr<memory::paged_memory_resource> resource_for_ptr_tables,
            std::shared_ptr<shuffle_info> info,
            [[maybe_unused]] std::size_t pointer_table_size = ptr_table_size
            ) :
            resource_for_records_(std::move(resource_for_records)),
            resource_for_ptr_tables_(std::move(resource_for_ptr_tables)),
            info_(std::move(info)),
            comparator_(info_->key_meta()),
            max_pointers_(pointer_table_size)
    {}

    /**
     * @brief write record to the input partition
     * @param record
     * @return whether flushing pointer table happens or not
     */
    bool write(accessor::record_ref record) {
        initialize_lazy();
        auto& table = pointer_tables_.back();
        table.emplace_back(records_->append(record.data(), info_->record_meta()->record_size()));
        if (table.capacity() == table.size()) {
            flush();
            return true;
        }
        return false;
    }

    /**
     * @brief finish current pointer table
     * @details the current internal pointer table is finalized and next write() will create one.
     */
    void flush() {
        if(!current_pointer_table_active_) return;
        auto sz = info_->record_meta()->record_size();
        auto& table = pointer_tables_.back();
        std::sort(table.begin(), table.end(), [&](auto const&x, auto const& y){
            return comparator_(info_->extract_key(accessor::record_ref(x, sz)),
                    info_->extract_key(accessor::record_ref(y, sz))) < 0;
        });
        current_pointer_table_active_ = false;
    }

    /**
     * @brief beginning iterator for pointer tables
     */
    iterator begin() {
        return pointer_tables_.begin();
    }

    /**
     * @brief ending iterator for pointer tables
     */
    iterator end() {
        return pointer_tables_.end();
    }

    /**
     * @brief returns the number of pointer tables
     */
    [[nodiscard]] std::size_t tables_count() const noexcept {
        return pointer_tables_.size();
    }

private:
    std::unique_ptr<memory::paged_memory_resource> resource_for_records_{};
    std::unique_ptr<memory::paged_memory_resource> resource_for_ptr_tables_{};
    std::shared_ptr<shuffle_info> info_{};
    std::unique_ptr<data::variable_length_data_region> records_{};
    pointer_tables_type pointer_tables_{};
    comparator comparator_{};
    bool current_pointer_table_active_{false};
    std::size_t max_pointers_{};

    void initialize_lazy() {
        if (!records_) {
            records_ = std::make_unique<data::variable_length_data_region>(resource_for_records_.get(), info_->record_meta()->record_alignment());
        }
        if(!current_pointer_table_active_) {
            auto& t = pointer_tables_.emplace_back(resource_for_ptr_tables_.get());
            t.reserve(max_pointers_);
            current_pointer_table_active_ = true;
        }
    }
};

}
