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

#include <jogasaki/executor/global.h>
#include <jogasaki/data/iterable_record_store.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/memory/monotonic_paged_memory_resource.h>

namespace jogasaki::data {

/**
 * @brief the store to hold result data from sql execution
 * @details this object can be used to store emit result record. This can be lazily initialized after construction with
 * the number of partitions. The same number of internal stores are kept. Iterator is provided
 * to iterate on merged result.
 */
class cache_align result_store {
public:
    using partition_type = data::iterable_record_store;
    using partitions_type = std::vector<std::unique_ptr<partition_type>>;
    using resources_type = std::vector<std::unique_ptr<memory::paged_memory_resource>>;

    /**
     * @brief create default object
     */
    result_store() = default;

    ~result_store() = default;
    result_store(result_store const& other) = delete;
    result_store& operator=(result_store const& other) = delete;
    result_store(result_store&& other) noexcept = default;
    result_store& operator=(result_store&& other) noexcept = default;

    /**
     * @brief initialize the result store
     * @details the number of partitions are not passed and partitions are later added with add_partition()
     * @param meta the record metadata stored in the store
     */
    void initialize(maybe_shared_ptr<meta::record_meta> meta);

    /**
     * @brief iterator of result store
     * @detail This iterates on merged results from partitions
     */
    class iterator {
    public:

        /// @brief iterator category
        using iterator_category = std::input_iterator_tag;

        /// @brief type of value
        using value_type = partition_type::value_type;

        /// @brief type of difference
        using difference_type = std::ptrdiff_t;

        /// @brief type of pointer
        using pointer = value_type*;

        /// @brief type of reference
        using reference = value_type&;

        /**
         * @brief construct empty iterator
         * @details this provides "empty" iterator that allows creating iterator even if result store is empty.
         * This iterator should be simply used as the placeholder when the result store is empty.
         * Dereferencing and incrementing causes undefined behavior.
         */
        iterator() noexcept;

        /**
         * @brief construct new iterator
         * @param container the target record store that the constructed object iterates
         * @param range indicates the range entry that the constructed iterator start iterating with
         */
        iterator(
            result_store const& container,
            std::size_t partition_index,
            partition_type::iterator it
        ) noexcept;

        /**
         * @brief increment iterator
         * @return reference after the increment
         */
        iterator& operator++();

        /**
         * @brief increment iterator
         * @return copy of the iterator before the increment
         */
        iterator const operator++(int);

        /**
         * @brief dereference the iterator
         * @return record ref to the record that the iterator is on
         */
        [[nodiscard]] value_type operator*() const;

        /**
         * @brief dereference the iterator and return record ref
         * @return record ref to the record that the iterator is on
         */
        [[nodiscard]] accessor::record_ref ref() const noexcept;

        /// @brief equivalent comparison
        constexpr bool operator==(iterator const& r) const noexcept {
            return this->container_ == r.container_ &&
                this->partition_index_ == r.partition_index_ &&
                this->it_ == r.it_;
        }

        /// @brief inequivalent comparison
        constexpr bool operator!=(const iterator& r) const noexcept {
            return !(*this == r);
        }

        /**
         * @brief appends string representation of the given value.
         * @param out the target output
         * @param value the target value
         * @return the output
         */
        friend inline std::ostream& operator<<(std::ostream& out, iterator value) {
            return out << std::hex
                << "container [" << value.container_
                <<"] partition_index [" << value.partition_index_
                << "] iterator [" << value.it_ << "]";
        }

    private:
        result_store const* container_{};
        std::size_t partition_index_{};
        partition_type::iterator it_;

        [[nodiscard]] bool valid() const noexcept;
    };

    /**
     * @brief returns whether the n-th internal store is valid
     */
    [[nodiscard]] bool exists(std::size_t index) const noexcept;

    /**
     * @brief accessor for n-th partition
     * @pre the existence should be ensured beforehand (e.g. by exists() call), otherwise the call causes UB
     */
    [[nodiscard]] partition_type& partition(std::size_t index) noexcept;

    /**
     * @brief accessor for n-th partition
     * @pre the existence should be ensured beforehand (e.g. by exists() call), otherwise the call causes UB
     */
    [[nodiscard]] partition_type const& partition(std::size_t index) const noexcept;

    /**
     * @brief initialize and set the capacity so that the store holds data from multiple partitions
     * @param partitions the number of partitions that generate result records. The same number of internal stores will
     * be prepared.
     * @param meta the metadata of the result record
     */
    void initialize(std::size_t partitions, maybe_shared_ptr<meta::record_meta> const& meta);

    /**
     * @brief add new partition to hold data
     * @returns partition index (0-origin)
     */
    std::size_t add_partition();

    /**
     * @brief clear the partition for the given index. The index is reserved, and will not be recycled.
     * @param index partition index (0-origin)
     */
    void clear_partition(std::size_t index);

    /**
     * @brief accessor to the metadata of the result record
     * @return record metadata
     */
    [[nodiscard]] maybe_shared_ptr<meta::record_meta> const& meta() const noexcept;

    /**
     * @brief return whether the result is empty or not
     * @details result set is considered empty either when it's not initialized with initialize(),
     * or no record has been append to any of the internal stores.
     */
    [[nodiscard]] bool empty() const noexcept;

    /**
     * @brief accessor for number of partitions
     */
    [[nodiscard]] std::size_t partitions() const noexcept;

    /**
     * @brief accessor to begin iterator
     * @details the iterator is intended for read-access of the result stores.
     * Iterator becomes invalid if the store is modified (e.g. by store(idx).append()).
     */
    [[nodiscard]] iterator begin() const noexcept;

    /**
     * @brief accessor to end iterator
     */
    [[nodiscard]] iterator end() const noexcept;

private:
    partitions_type partitions_{};
    resources_type result_record_resources_{};
    resources_type result_varlen_resources_{};
    maybe_shared_ptr<meta::record_meta> meta_{};

    void add_partition_internal(maybe_shared_ptr<meta::record_meta> const& meta);
};

}

