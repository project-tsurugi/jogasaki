/*
 * Copyright 2018-2024 Project Tsurugi.
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

#include <array>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iterator>
#include <memory>
#include <ostream>
#include <utility>
#include <vector>

#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/print_support.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_traits.h>
#include <jogasaki/utils/assert.h>
#include <jogasaki/utils/fail.h>
#include <jogasaki/utils/interference_size.h>

namespace jogasaki::data {

using takatori::util::maybe_shared_ptr;

namespace details {

/**
 * @brief iterator for the stored records
 * @tparam the value type
 */
template <class T>
class iterator {
public:

    /// @brief iterator category
    using iterator_category = std::input_iterator_tag;

    /// @brief type of value
    using value_type = T;

    /// @brief type of difference
    using difference_type = std::ptrdiff_t;

    /// @brief type of pointer
    using value_pointer = value_type*;

    /// @brief type of reference
    using reference = value_type&;

    /// @brief block type holding bit-packed null flags (8 flags per block)
    using null_block_type = std::uint8_t;

    /// @brief number of null flags packed into a single block
    static constexpr std::size_t flags_per_block = 8;

    static_assert(flags_per_block == sizeof(null_block_type) * 8);

    struct range {
        range(value_pointer b, value_pointer e) : b_(b), e_(e) {}
        value_pointer b_; //NOLINT
        value_pointer e_; //NOLINT
    };

    /// @brief type for list of ranges
    using range_list = std::vector<range>;

    /// @brief type for list of ranges
    using range_list_iterator = typename range_list::const_iterator;

    /// @brief range of contiguous null flag blocks
    struct null_range {
        null_range(null_block_type* b, null_block_type* e) : b_(b), e_(e) {}
        null_block_type* b_; //NOLINT
        null_block_type* e_; //NOLINT
    };

    /// @brief type for list of null flag ranges
    using null_range_list = std::vector<null_range>;

    /// @brief type for iterating null flag ranges
    using null_range_list_iterator = typename null_range_list::const_iterator;

    /**
     * @brief create empty object
     */
    iterator() = default;

    /**
     * @brief construct new iterator
     * @param ranges indicates the ranges container
     * @param current indicates the range entry that the constructed iterator starts iterating with.
     * This is expected to be either @c ranges.begin() (to build a begin iterator) or @c ranges.end()
     * (to build an end iterator). Passing any other range entry is not supported.
     * @param null_ranges indicates the null flag ranges container
     * @throws std::logic_error if @c current is neither @c ranges.begin() nor @c ranges.end().
     */
    iterator(
        range_list const& ranges,
        range_list_iterator current,
        null_range_list const& null_ranges
    ) :
        value_base_(ranges.end() == current ? nullptr : current->b_),
        value_current_(current),
        null_base_(
            (ranges.end() == current || null_ranges.empty()) ? nullptr : null_ranges.begin()->b_),
        null_current_(ranges.end() == current ? null_ranges.end() : null_ranges.begin()),
        value_ranges_(std::addressof(ranges)),
        null_ranges_(std::addressof(null_ranges))
    {
        assert_with_exception(current == ranges.begin() || current == ranges.end());
    }

    /**
     * @brief increment iterator
     * @return reference after the increment
     */
    iterator& operator++() {
        ++value_offset_;
        if (value_offset_ >= static_cast<std::size_t>(value_current_->e_ - value_current_->b_)) {
            ++value_current_;
            if(value_current_ != value_ranges_->end()) {
                value_base_ = value_current_->b_;
            } else {
                value_base_ = nullptr;
            }
            value_offset_ = 0;
        }
        // advance the null flag cursor independently from the value cursor, mirroring the value
        // cursor above: null_offset_ is the flag offset within the current null range and may span
        // multiple 8-flag blocks
        if (null_base_ != nullptr) {
            ++null_offset_;
            auto const range_flags =
                static_cast<std::size_t>(null_current_->e_ - null_current_->b_) * flags_per_block;
            if (null_offset_ >= range_flags) {
                ++null_current_;
                if (null_current_ != null_ranges_->end()) {
                    null_base_ = null_current_->b_;
                } else {
                    null_base_ = nullptr;
                }
                null_offset_ = 0;
            }
        }
        return *this;
    }

    /**
     * @brief increment iterator
     * @return copy of the iterator before the increment
     */
    iterator const operator++(int) {  //NOLINT
        auto it = *this;
        this->operator++();
        return it;
    }

    /**
     * @brief returns if the iterator is pointing valid value
     */
    [[nodiscard]] bool valid() const noexcept {
        return value_base_ != nullptr;
    }

    /**
     * @brief dereference the iterator
     * @return record ref to the record that the iterator is on
     */
    [[nodiscard]] value_type operator*() {
        assert_with_exception(valid());
        return *(value_base_+value_offset_);
    }

    [[nodiscard]] bool is_null() const {
        assert_with_exception(valid());
        if (null_base_ == nullptr) {
            return false;
        }
        auto const* block = null_base_ + null_offset_ / flags_per_block;
        return ((*block >> (null_offset_ % flags_per_block)) & 1U) != 0U;
    }

    /// @brief equivalent comparison
    constexpr bool operator==(iterator const& r) const noexcept {
        // Calculate the current position based on the value part portion of the iterator.
        // The null part portion of the iterator is not considered for equality comparison
        // because the null part is expected to be in sync with the value part.
        return this->value_base_ == r.value_base_ &&
            this->value_ranges_ == r.value_ranges_ &&
            this->null_ranges_ == r.null_ranges_ &&
            this->value_current_ == r.value_current_ &&
            this->value_offset_ == r.value_offset_;
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
            << "ranges [" << takatori::util::print_support(value.value_ranges_)
            <<"] current range [" << takatori::util::print_support(value.value_current_)
            << "] base [" << value.value_base_ << "]"
            << "] offset [" << value.value_offset_ << "]"
            << "] null_base [" << static_cast<void const*>(value.null_base_) << "]"
            << "] null_offset [" << value.null_offset_ << "]";
    }

private:
    // order hot fields first
    std::size_t value_offset_{}; // offset based on value_base_
    std::size_t null_offset_{}; // bit offset based on null_base_. Can be greater than flags_per_block

    value_pointer value_base_{};
    range_list_iterator value_current_{};
    null_block_type const* null_base_{};
    null_range_list_iterator null_current_{};

    range_list const* value_ranges_{};
    null_range_list const* null_ranges_{};
};

class cache_align typed_store {
public:
    using kind = meta::field_type_kind;
    /**
     * @brief create empty object
     */
    typed_store() = default;

    virtual ~typed_store() = default;

    typed_store(typed_store const& other) = default;
    typed_store& operator=(typed_store const& other) = default;
    typed_store(typed_store&& other) noexcept = default;
    typed_store& operator=(typed_store&& other) noexcept = default;

    /**
     * @brief append null to the store
     */
    virtual void append_null() = 0;

    /**
     * @brief copy and store the value
     * For varlen data such as text, the data on the varlen buffer will be copied using varlen resource assigned to
     * this object unless it's nullptr.
     * @param value the value to be added
     */
    virtual void append_boolean(runtime_t<kind::boolean> value) = 0;
    virtual void append_int4(runtime_t<kind::int4> value) = 0;
    virtual void append_int8(runtime_t<kind::int8> value) = 0;
    virtual void append_float4(runtime_t<kind::float4> value) = 0;
    virtual void append_float8(runtime_t<kind::float8> value) = 0;
    virtual void append_decimal(runtime_t<kind::decimal> value) = 0;
    virtual void append_character(runtime_t<kind::character> value) = 0;
    virtual void append_octet(runtime_t<kind::octet> value) = 0;
    virtual void append_date(runtime_t<kind::date> value) = 0;
    virtual void append_time_of_day(runtime_t<kind::time_of_day> value) = 0;
    virtual void append_time_point(runtime_t<kind::time_point> value) = 0;

    [[nodiscard]] virtual std::size_t count() const noexcept = 0;

    /**
     * @return whether the store is empty
     */
    [[nodiscard]] virtual bool empty() const noexcept = 0;

    /**
     * @brief getter of begin iterator
     * @return iterator at the beginning of the store
     * @warning the returned iterator will be invalid when new append() is called.
     */

    [[nodiscard]] virtual iterator<runtime_t<kind::boolean>> begin_boolean() const = 0;
    [[nodiscard]] virtual iterator<runtime_t<kind::int4>> begin_int4() const = 0;
    [[nodiscard]] virtual iterator<runtime_t<kind::int8>> begin_int8() const = 0;
    [[nodiscard]] virtual iterator<runtime_t<kind::float4>> begin_float4() const = 0;
    [[nodiscard]] virtual iterator<runtime_t<kind::float8>> begin_float8() const = 0;
    [[nodiscard]] virtual iterator<runtime_t<kind::decimal>> begin_decimal() const = 0;
    [[nodiscard]] virtual iterator<runtime_t<kind::character>> begin_character() const = 0;
    [[nodiscard]] virtual iterator<runtime_t<kind::octet>> begin_octet() const = 0;
    [[nodiscard]] virtual iterator<runtime_t<kind::date>> begin_date() const = 0;
    [[nodiscard]] virtual iterator<runtime_t<kind::time_of_day>> begin_time_of_day() const = 0;
    [[nodiscard]] virtual iterator<runtime_t<kind::time_point>> begin_time_point() const = 0;

    /**
     * @brief getter of end iterator
     * @return iterator at the end of the store
     * @warning the returned iterator will be invalid when new append() is called
     */
    [[nodiscard]] virtual iterator<runtime_t<kind::boolean>> end_boolean() const = 0;
    [[nodiscard]] virtual iterator<runtime_t<kind::int4>> end_int4() const = 0;
    [[nodiscard]] virtual iterator<runtime_t<kind::int8>> end_int8() const = 0;
    [[nodiscard]] virtual iterator<runtime_t<kind::float4>> end_float4() const = 0;
    [[nodiscard]] virtual iterator<runtime_t<kind::float8>> end_float8() const = 0;
    [[nodiscard]] virtual iterator<runtime_t<kind::decimal>> end_decimal() const = 0;
    [[nodiscard]] virtual iterator<runtime_t<kind::character>> end_character() const = 0;
    [[nodiscard]] virtual iterator<runtime_t<kind::octet>> end_octet() const = 0;
    [[nodiscard]] virtual iterator<runtime_t<kind::date>> end_date() const = 0;
    [[nodiscard]] virtual iterator<runtime_t<kind::time_of_day>> end_time_of_day() const = 0;
    [[nodiscard]] virtual iterator<runtime_t<kind::time_point>> end_time_point() const = 0;

    /**
     * @brief reset the store clearing all values
     */
    virtual void reset() noexcept = 0;
};

/**
 * @brief value store implementation
 */
template <class T>
class cache_align typed_value_store : public typed_store {
public:
    /// @brief pointer type
    using value_type = T;

    using value_pointer = value_type*;

    using null_block_type = typename iterator<T>::null_block_type;

    using null_range_list = typename iterator<T>::null_range_list;

    static constexpr std::size_t flags_per_block = iterator<T>::flags_per_block;

    using range_list = typename iterator<T>::range_list;

    using kind = meta::field_type_kind;

    constexpr static std::size_t value_length = sizeof(T);
    constexpr static std::size_t value_alignment = alignof(T);

    /**
     * @brief create empty object
     */
    typed_value_store() = default;

    /**
     * @brief create new object
     * @param record_resource memory resource backing this store
     * @param varlen_resource varlen memory resource for the variable length data stored in this store.
     * Specify nullptr if the value type is not of variable length.
     * @param nulls_resource memory resource backing null flags. Specify nullptr if the value never becomes null.
     */
    typed_value_store(
        memory::paged_memory_resource* record_resource,
        memory::paged_memory_resource* varlen_resource,
        memory::paged_memory_resource* nulls_resource
    ) :
        resource_(record_resource),
        varlen_resource_(varlen_resource),
        nulls_resource_(nulls_resource)
    {}

    /**
     * @brief append null
     * @pre nulls_resource must be specified on construction
     */
    void append_null() override {
        assert_with_exception(nulls_resource_ != nullptr);
        internal_append(nullptr);
    }

    /**
     * @brief copy and store the value
     * For varlen data such as text, the data on the varlen buffer will be copied using varlen resource assigned to
     * this object unless it's nullptr.
     * @param value added to the store
     */
    void append_boolean(runtime_t<kind::boolean> value) override {
        if constexpr (std::is_same_v<T, runtime_t<kind::boolean>>) { //NOLINT
            internal_append(&value);
        }
    }

    void append_int4(runtime_t<kind::int4> value) override {
        if constexpr (std::is_same_v<T, runtime_t<kind::int4>>) { //NOLINT
            internal_append(&value);
        }
    }

    void append_int8(runtime_t<kind::int8> value) override {
        if constexpr (std::is_same_v<T, runtime_t<kind::int8>>) { //NOLINT
            internal_append(&value);
        }
    }

    void append_float4(runtime_t<kind::float4> value) override {
        if constexpr (std::is_same_v<T, runtime_t<kind::float4>>) { //NOLINT
            internal_append(&value);
        }
    }

    void append_float8(runtime_t<kind::float8> value) override {
        if constexpr (std::is_same_v<T, runtime_t<kind::float8>>) { //NOLINT
            internal_append(&value);
        }
    }

    void append_decimal(runtime_t<kind::decimal> value) override {
        if constexpr (std::is_same_v<T, runtime_t<kind::decimal>>) { //NOLINT
            internal_append(&value);
        }
    }

    void append_character(runtime_t<kind::character> value) override {
        if constexpr (std::is_same_v<T, runtime_t<kind::character>>) { //NOLINT
            internal_append(&value);
        }
    }

    void append_octet(runtime_t<kind::octet> value) override {
        if constexpr (std::is_same_v<T, runtime_t<kind::octet>>) { //NOLINT
            internal_append(&value);
        }
    }

    void append_date(runtime_t<kind::date> value) override {
        if constexpr (std::is_same_v<T, runtime_t<kind::date>>) { //NOLINT
            internal_append(&value);
        }
    }

    void append_time_of_day(runtime_t<kind::time_of_day> value) override {
        if constexpr (std::is_same_v<T, runtime_t<kind::time_of_day>>) { //NOLINT
            internal_append(&value);
        }
    }

    void append_time_point(runtime_t<kind::time_point> value) override {
        if constexpr (std::is_same_v<T, runtime_t<kind::time_point>>) { //NOLINT
            internal_append(&value);
        }
    }

    [[nodiscard]] std::size_t count() const noexcept override {
        return count_;
    }

    [[nodiscard]] bool empty() const noexcept override {
        return count_ == 0;
    }

    /**
     * @brief getter of begin iterator
     * @return iterator at the beginning of the store
     * @warning the returned iterator will be invalid when new append() is called.
     */
    [[nodiscard]] iterator<runtime_t<kind::boolean>> begin_boolean() const override {
        if constexpr (std::is_same_v<T, runtime_t<kind::boolean>>) { //NOLINT
            return iterator<T>{value_ranges_, value_ranges_.begin(), null_ranges_};
        } else { //NOLINT
            return {};
        }
    }

    [[nodiscard]] iterator<runtime_t<kind::int4>> begin_int4() const override {
        if constexpr (std::is_same_v<T, runtime_t<kind::int4>>) { //NOLINT
            return iterator<T>{value_ranges_, value_ranges_.begin(), null_ranges_};
        } else { //NOLINT
            return {};
        }
    }

    [[nodiscard]] iterator<runtime_t<kind::int8>> begin_int8() const override {
        if constexpr (std::is_same_v<T, runtime_t<kind::int8>>) {  //NOLINT
            return iterator<T>{value_ranges_, value_ranges_.begin(), null_ranges_};
        } else {  //NOLINT
            return {};
        }
    }

    [[nodiscard]] iterator<runtime_t<kind::float4>> begin_float4() const override {
        if constexpr (std::is_same_v<T, runtime_t<kind::float4>>) {  //NOLINT
            return iterator<T>{value_ranges_, value_ranges_.begin(), null_ranges_};
        } else {  //NOLINT
            return {};
        }
    }

    [[nodiscard]] iterator<runtime_t<kind::float8>> begin_float8() const override {
        if constexpr (std::is_same_v<T, runtime_t<kind::float8>>) {  //NOLINT
            return iterator<T>{value_ranges_, value_ranges_.begin(), null_ranges_};
        } else {  //NOLINT
            return {};
        }
    }

    [[nodiscard]] iterator<runtime_t<kind::decimal>> begin_decimal() const override {
        if constexpr (std::is_same_v<T, runtime_t<kind::decimal>>) {  //NOLINT
            return iterator<T>{value_ranges_, value_ranges_.begin(), null_ranges_};
        } else {  //NOLINT
            return {};
        }
    }

    [[nodiscard]] iterator<runtime_t<kind::character>> begin_character() const override {
        if constexpr (std::is_same_v<T, runtime_t<kind::character>>) {  //NOLINT
            return iterator<T>{value_ranges_, value_ranges_.begin(), null_ranges_};
        } else {  //NOLINT
            return {};
        }
    }

    [[nodiscard]] iterator<runtime_t<kind::octet>> begin_octet() const override {
        if constexpr (std::is_same_v<T, runtime_t<kind::octet>>) {  //NOLINT
            return iterator<T>{value_ranges_, value_ranges_.begin(), null_ranges_};
        } else {  //NOLINT
            return {};
        }
    }

    [[nodiscard]] iterator<runtime_t<kind::date>> begin_date() const override {
        if constexpr (std::is_same_v<T, runtime_t<kind::date>>) {  //NOLINT
            return iterator<T>{value_ranges_, value_ranges_.begin(), null_ranges_};
        } else {  //NOLINT
            return {};
        }
    }

    [[nodiscard]] iterator<runtime_t<kind::time_of_day>> begin_time_of_day() const override {
        if constexpr (std::is_same_v<T, runtime_t<kind::time_of_day>>) {  //NOLINT
            return iterator<T>{value_ranges_, value_ranges_.begin(), null_ranges_};
        } else {  //NOLINT
            return {};
        }
    }

    [[nodiscard]] iterator<runtime_t<kind::time_point>> begin_time_point() const override {
        if constexpr (std::is_same_v<T, runtime_t<kind::time_point>>) {  //NOLINT
            return iterator<T>{value_ranges_, value_ranges_.begin(), null_ranges_};
        } else {  //NOLINT
            return {};
        }
    }

    /**
     * @brief getter of end iterator
     * @return iterator at the end of the store
     * @warning the returned iterator will be invalid when new append() is called
     */
    [[nodiscard]] iterator<runtime_t<kind::boolean>> end_boolean() const override {
        if constexpr (std::is_same_v<T, runtime_t<kind::boolean>>) {  //NOLINT
            return iterator<T>{value_ranges_, value_ranges_.end(), null_ranges_};
        } else {  //NOLINT
            return {};
        }
    }

    [[nodiscard]] iterator<runtime_t<kind::int4>> end_int4() const override {
        if constexpr (std::is_same_v<T, runtime_t<kind::int4>>) {  //NOLINT
            return iterator<T>{value_ranges_, value_ranges_.end(), null_ranges_};
        } else {  //NOLINT
            return {};
        }
    }

    [[nodiscard]] iterator<runtime_t<kind::int8>> end_int8() const override {
        if constexpr (std::is_same_v<T, runtime_t<kind::int8>>) {  //NOLINT
            return iterator<T>{value_ranges_, value_ranges_.end(), null_ranges_};
        } else {  //NOLINT
            return {};
        }
    }

    [[nodiscard]] iterator<runtime_t<kind::float4>> end_float4() const override {
        if constexpr (std::is_same_v<T, runtime_t<kind::float4>>) {  //NOLINT
            return iterator<T>{value_ranges_, value_ranges_.end(), null_ranges_};
        } else {  //NOLINT
            return {};
        }
    }

    [[nodiscard]] iterator<runtime_t<kind::float8>> end_float8() const override {
        if constexpr (std::is_same_v<T, runtime_t<kind::float8>>) {  //NOLINT
            return iterator<T>{value_ranges_, value_ranges_.end(), null_ranges_};
        } else {  //NOLINT
            return {};
        }
    }

    [[nodiscard]] iterator<runtime_t<kind::decimal>> end_decimal() const override {
        if constexpr (std::is_same_v<T, runtime_t<kind::decimal>>) {  //NOLINT
            return iterator<T>{value_ranges_, value_ranges_.end(), null_ranges_};
        } else {  //NOLINT
            return {};
        }
    }

    [[nodiscard]] iterator<runtime_t<kind::character>> end_character() const override {
        if constexpr (std::is_same_v<T, runtime_t<kind::character>>) {  //NOLINT
            return iterator<T>{value_ranges_, value_ranges_.end(), null_ranges_};
        } else {  //NOLINT
            return {};
        }
    }

    [[nodiscard]] iterator<runtime_t<kind::octet>> end_octet() const override {
        if constexpr (std::is_same_v<T, runtime_t<kind::octet>>) {  //NOLINT
            return iterator<T>{value_ranges_, value_ranges_.end(), null_ranges_};
        } else {  //NOLINT
            return {};
        }
    }

    [[nodiscard]] iterator<runtime_t<kind::date>> end_date() const override {
        if constexpr (std::is_same_v<T, runtime_t<kind::date>>) {  //NOLINT
            return iterator<T>{value_ranges_, value_ranges_.end(), null_ranges_};
        } else {  //NOLINT
            return {};
        }
    }

    [[nodiscard]] iterator<runtime_t<kind::time_of_day>> end_time_of_day() const override {
        if constexpr (std::is_same_v<T, runtime_t<kind::time_of_day>>) {  //NOLINT
            return iterator<T>{value_ranges_, value_ranges_.end(), null_ranges_};
        } else {  //NOLINT
            return {};
        }
    }

    [[nodiscard]] iterator<runtime_t<kind::time_point>> end_time_point() const override {
        if constexpr (std::is_same_v<T, runtime_t<kind::time_point>>) {  //NOLINT
            return iterator<T>{value_ranges_, value_ranges_.end(), null_ranges_};
        } else {  //NOLINT
            return {};
        }
    }

    /**
     * @brief reset store state except the state managed by memory resource
     * @details To keep consistency, caller needs to reset or release appropriately (e.g. deallocate to some check point)
     * the memory resources passed to constructor when calling this function.
     */
    void reset() noexcept override {
        count_ = 0;
        value_prev_ = nullptr;
        value_ranges_.clear();
        null_ranges_.clear();
        null_cur_block_ = nullptr;
        null_next_bit_ = 0;
    }

private:
    // write-hot scalars first
    std::size_t count_{};
    value_pointer value_prev_{};
    std::size_t null_next_bit_{};
    null_block_type* null_cur_block_{};

    range_list value_ranges_{};
    null_range_list null_ranges_{};

    memory::paged_memory_resource* resource_{};
    memory::paged_memory_resource* varlen_resource_{};
    memory::paged_memory_resource* nulls_resource_{};

    void internal_append_null_flag(bool arg) {
        assert_with_exception(nulls_resource_ != nullptr);
        if (null_next_bit_ == 0) {
            // a new block is needed to store the next 8 flags
            auto* p = static_cast<null_block_type*>(
                nulls_resource_->allocate(sizeof(null_block_type), alignof(null_block_type)));
            assert_with_exception(p != nullptr);
            *p = 0;
            if (null_cur_block_ == nullptr || p != null_cur_block_ + 1) { //NOLINT
                // not contiguous with the previous block (e.g. crossing a page boundary):
                // start a new range so that null flags are not required to be contiguous
                null_ranges_.emplace_back(p, nullptr);
            }
            null_ranges_.back().e_ = p + 1; //NOLINT
            null_cur_block_ = p;
        }
        if (arg) {
            *null_cur_block_ |= static_cast<null_block_type>(1U << null_next_bit_);
        }
        null_next_bit_ = (null_next_bit_ + 1) % flags_per_block;
    }

    void internal_append(void* src) {
        // Even if src is null, the value space is kept to calculate the offset.
        // TODO optimize to save the space for values when the value is null
        auto* p = static_cast<value_pointer>(resource_->allocate(value_length, value_alignment));
        assert_with_exception(p != nullptr);
        if (src != nullptr) {
            if constexpr (std::is_same_v<T, accessor::text>) {  //NOLINT
                assert_with_exception(varlen_resource_ != nullptr);
                accessor::text t{varlen_resource_, *reinterpret_cast<accessor::text*>(src)}; //NOLINT
                std::memcpy(p, &t, value_length);
            } else if constexpr (std::is_same_v<T, accessor::binary>) {  //NOLINT
                assert_with_exception(varlen_resource_ != nullptr);
                accessor::binary t{varlen_resource_, *reinterpret_cast<accessor::binary*>(src)}; //NOLINT
                std::memcpy(p, &t, value_length);
            } else {  //NOLINT
                std::memcpy(p, src, value_length);
            }
        }
        if (nulls_resource_ != nullptr) {
            internal_append_null_flag(src == nullptr);
        }
        ++count_;

        if (value_prev_ == nullptr || p != value_prev_ + 1) { //NOLINT
            // starting new range
            value_ranges_.emplace_back(p, nullptr);
        }
        value_ranges_.back().e_ = p + 1; //NOLINT
        value_prev_ = p;
    }
};

}

/**
 * @brief value store
 * @details auto-expand append-only container for field values. This object holds any number of values
 * @note the backing memory resource is expected to be used almost exclusively for this store.
 * Even if the resource is shared by others and the appended records are not in the adjacent position,
 * this class handles that case, but the ranges become granule, the number of ranges become large and
 * the performance possibly gets affected.
 */
class cache_align value_store {
public:
    using kind = meta::field_type_kind;

    /**
     * @brief create empty object
     */
    value_store() = default;

    /**
     * @brief create new object
     * @param type type of the value stored
     * @param resource resource used to store the value
     * @param varlen_resource resource used to store the varlen data referenced from value.
     * Specify nullptr if the value type is not variable length.
     * @param nulls_resource memory resource backing null flags. Specify nullptr if the value never becomes null.
     */
    value_store(
        meta::field_type const& type,
        memory::paged_memory_resource* resource,
        memory::paged_memory_resource* varlen_resource,
        memory::paged_memory_resource* nulls_resource = nullptr
    ) :
        type_(type),
        base_(make_typed_store(type, resource, varlen_resource, nulls_resource))
    {}

    /**
     * @brief copy and store the value
     * For varlen data such as text, the data on the varlen buffer will be copied using varlen resource assigned to
     * this object unless it's nullptr.
     * @tparam T the runtime type of the field value
     * @param value the value to append
     */
    template <class T>
    void append(T value) {
        if constexpr(std::is_same_v<T, runtime_t<kind::boolean>>) {  //NOLINT
            base_->append_boolean(value);
        } else if constexpr(std::is_same_v<T, runtime_t<kind::int4>>) {  //NOLINT
            base_->append_int4(value);
        } else if constexpr(std::is_same_v<T, runtime_t<kind::int8>>) {  //NOLINT
            base_->append_int8(value);
        } else if constexpr(std::is_same_v<T, runtime_t<kind::float4>>) {  //NOLINT
            base_->append_float4(value);
        } else if constexpr(std::is_same_v<T, runtime_t<kind::float8>>) {  //NOLINT
            base_->append_float8(value);
        } else if constexpr(std::is_same_v<T, runtime_t<kind::decimal>>) {  //NOLINT
            base_->append_decimal(value);
        } else if constexpr(std::is_same_v<T, runtime_t<kind::character>>) {  //NOLINT
            base_->append_character(value);
        } else if constexpr(std::is_same_v<T, runtime_t<kind::octet>>) {  //NOLINT
            base_->append_octet(value);
        } else if constexpr(std::is_same_v<T, runtime_t<kind::date>>) {  //NOLINT
            base_->append_date(value);
        } else if constexpr(std::is_same_v<T, runtime_t<kind::time_of_day>>) {  //NOLINT
            base_->append_time_of_day(value);
        } else if constexpr(std::is_same_v<T, runtime_t<kind::time_point>>) {  //NOLINT
            base_->append_time_point(value);
        } else {
            fail_with_exception();
        }
    }

    void append_null() {
        base_->append_null();
    }

    [[nodiscard]] std::size_t count() const noexcept {
        return base_->count();
    }

    [[nodiscard]] bool empty() const noexcept {
        return base_->empty();
    }

    /**
     * @brief getter of begin iterator
     * @return iterator at the beginning of the store
     * @warning the returned iterator will be invalid when new append() is called.
     */
    template <class T>
    [[nodiscard]] details::iterator<T> begin() const {
        if constexpr(std::is_same_v<T, runtime_t<kind::boolean>>) {  //NOLINT
            return base_->begin_boolean();
        } else if constexpr(std::is_same_v<T, runtime_t<kind::int4>>) {  //NOLINT
            return base_->begin_int4();
        } else if constexpr(std::is_same_v<T, runtime_t<kind::int8>>) {  //NOLINT
            return base_->begin_int8();
        } else if constexpr(std::is_same_v<T, runtime_t<kind::float4>>) {  //NOLINT
            return base_->begin_float4();
        } else if constexpr(std::is_same_v<T, runtime_t<kind::float8>>) {  //NOLINT
            return base_->begin_float8();
        } else if constexpr(std::is_same_v<T, runtime_t<kind::decimal>>) {  //NOLINT
            return base_->begin_decimal();
        } else if constexpr(std::is_same_v<T, runtime_t<kind::character>>) {  //NOLINT
            return base_->begin_character();
        } else if constexpr(std::is_same_v<T, runtime_t<kind::octet>>) {  //NOLINT
            return base_->begin_octet();
        } else if constexpr(std::is_same_v<T, runtime_t<kind::date>>) {  //NOLINT
            return base_->begin_date();
        } else if constexpr(std::is_same_v<T, runtime_t<kind::time_of_day>>) {  //NOLINT
            return base_->begin_time_of_day();
        } else if constexpr(std::is_same_v<T, runtime_t<kind::time_point>>) {  //NOLINT
            return base_->begin_time_point();
        } else {
            fail_with_exception();
        }
    }

    /**
     * @brief getter of end iterator
     * @return iterator at the end of the store
     * @warning the returned iterator will be invalid when new append() is called
     */
    template <class T>
    [[nodiscard]] details::iterator<T> end() const {
        if constexpr(std::is_same_v<T, runtime_t<kind::boolean>>) {  //NOLINT
            return base_->end_boolean();
        } else if constexpr(std::is_same_v<T, runtime_t<kind::int4>>) {  //NOLINT
            return base_->end_int4();
        } else if constexpr(std::is_same_v<T, runtime_t<kind::int8>>) {  //NOLINT
            return base_->end_int8();
        } else if constexpr(std::is_same_v<T, runtime_t<kind::float4>>) {  //NOLINT
            return base_->end_float4();
        } else if constexpr(std::is_same_v<T, runtime_t<kind::float8>>) {  //NOLINT
            return base_->end_float8();
        } else if constexpr(std::is_same_v<T, runtime_t<kind::decimal>>) {  //NOLINT
            return base_->end_decimal();
        } else if constexpr(std::is_same_v<T, runtime_t<kind::character>>) {  //NOLINT
            return base_->end_character();
        } else if constexpr(std::is_same_v<T, runtime_t<kind::octet>>) {  //NOLINT
            return base_->end_octet();
        } else if constexpr(std::is_same_v<T, runtime_t<kind::date>>) {  //NOLINT
            return base_->end_date();
        } else if constexpr(std::is_same_v<T, runtime_t<kind::time_of_day>>) {  //NOLINT
            return base_->end_time_of_day();
        } else if constexpr(std::is_same_v<T, runtime_t<kind::time_point>>) {  //NOLINT
            return base_->end_time_point();
        } else {
            fail_with_exception();
        }
    }

    void reset() noexcept {
        base_->reset();
    }

    /**
     * @brief accessor to metadata
     * @return record meta held by this object
     */
    [[nodiscard]] meta::field_type const& type() const noexcept {
        return type_;
    }

private:
    meta::field_type type_{};
    std::unique_ptr<details::typed_store> base_{};

    std::unique_ptr<details::typed_store> make_typed_store(
        meta::field_type const& type,
        memory::paged_memory_resource* record_resource,
        memory::paged_memory_resource* varlen_resource,
        memory::paged_memory_resource* nulls_resource
    ) {
        switch(type.kind()) {
            case kind::boolean: return std::make_unique<details::typed_value_store<runtime_t<kind::boolean>>>(record_resource, varlen_resource, nulls_resource);
            case kind::int4: return std::make_unique<details::typed_value_store<runtime_t<kind::int4>>>(record_resource, varlen_resource, nulls_resource);
            case kind::int8: return std::make_unique<details::typed_value_store<runtime_t<kind::int8>>>(record_resource, varlen_resource, nulls_resource);
            case kind::float4: return std::make_unique<details::typed_value_store<runtime_t<kind::float4>>>(record_resource, varlen_resource, nulls_resource);
            case kind::float8: return std::make_unique<details::typed_value_store<runtime_t<kind::float8>>>(record_resource, varlen_resource, nulls_resource);
            case kind::decimal: return std::make_unique<details::typed_value_store<runtime_t<kind::decimal>>>(record_resource, varlen_resource, nulls_resource);
            case kind::character: return std::make_unique<details::typed_value_store<runtime_t<kind::character>>>(record_resource, varlen_resource, nulls_resource);
            case kind::octet: return std::make_unique<details::typed_value_store<runtime_t<kind::octet>>>(record_resource, varlen_resource, nulls_resource);
            case kind::date: return std::make_unique<details::typed_value_store<runtime_t<kind::date>>>(record_resource, varlen_resource, nulls_resource);
            case kind::time_of_day: return std::make_unique<details::typed_value_store<runtime_t<kind::time_of_day>>>(record_resource, varlen_resource, nulls_resource);
            case kind::time_point: return std::make_unique<details::typed_value_store<runtime_t<kind::time_point>>>(record_resource, varlen_resource, nulls_resource);
            default: fail_with_exception();
        }
    }
};

} // namespace
