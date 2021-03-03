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

#include <array>
#include <vector>
#include <cstring>

#include <takatori/util/print_support.h>

#include <jogasaki/data/record_store.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/utils/interference_size.h>
#include <jogasaki/accessor/record_ref.h>

namespace jogasaki::data {

using takatori::util::maybe_shared_ptr;
using takatori::util::fail;

template <meta::field_type_kind Kind>
using rtype = typename meta::field_type_traits<Kind>::runtime_type;

namespace details {

/**
 * @brief iterator for the stored records
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

    struct range {
        range(value_pointer b, value_pointer e) : b_(b), e_(e) {}
        value_pointer b_; //NOLINT
        value_pointer e_; //NOLINT
    };

    /// @brief type for list of ranges
    using range_list = std::vector<range>;

    /// @brief type for list of ranges
    using range_list_iterator = typename range_list::const_iterator;

    /**
     * @brief create empty object
     */
    iterator() = default;

    /**
     * @brief construct new iterator
     * @param container the target record store that the constructed object iterates
     * @param range indicates the range entry that the constructed iterator start iterating with
     */
    iterator(
        range_list const& ranges,
        range_list_iterator range,
        value_pointer base,
        std::size_t offset
    ) :
        ranges_(std::addressof(ranges)),
        range_(range),
        base_(base),
        offset_(offset)
    {}
    /**
     * @brief construct new iterator
     * @param container the target record store that the constructed object iterates
     * @param range indicates the range entry that the constructed iterator start iterating with
     */
    iterator(
        range_list const& ranges,
        range_list_iterator range
    ) :
        iterator(
            ranges,
            range,
            ranges.end() == range ? nullptr : range->b_,
            0
        )
    {}

    /**
     * @brief increment iterator
     * @return reference after the increment
     */
    iterator& operator++() {
        ++offset_;
        if (offset_ >= static_cast<std::size_t>(range_->e_ - range_->b_)) {
            ++range_;
            if(range_ != ranges_->end()) {
                base_ = range_->b_;
            } else {
                base_ = nullptr;
            }
            offset_ = 0;
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
        return base_ != nullptr;
    }

    /**
     * @brief dereference the iterator
     * @return record ref to the record that the iterator is on
     */
    [[nodiscard]] value_type operator*() {
        BOOST_ASSERT(valid());  //NOLINT
        return *(base_+offset_);
    }

    [[nodiscard]] bool is_null() const noexcept {
        // not supported yet
        fail();
    }

    /// @brief equivalent comparison
    constexpr bool operator==(iterator const& r) const noexcept {
        return this->base_ == r.base_ &&
            this->ranges_ == r.ranges_ &&
            this->range_ == r.range_ &&
            this->offset_ == r.offset_;
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
            << "ranges [" << takatori::util::print_support(value.ranges_)
            <<"] current range [" << takatori::util::print_support(value.range_)
            << "] base [" << value.base_ << "]"
            << "] offset [" << value.offset_ << "]";
    }

private:
    range_list const* ranges_{};
    range_list_iterator range_{};
    value_pointer base_{};
    std::size_t offset_{};
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
     * @brief copy and store the value
     * For varlen data such as text, the data on the varlen buffer will be copied using varlen resource assigned to
     * this object unless it's nullptr.
     * @param record source of the record added to this container
     * @return record ref to the stored record
     */
    virtual void append_int4(rtype<kind::int4> t) = 0;
    virtual void append_int8(rtype<kind::int8> t) = 0;
    virtual void append_float4(rtype<kind::float4> t) = 0;
    virtual void append_float8(rtype<kind::float8> t) = 0;
    virtual void append_character(rtype<kind::character> t) = 0;

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

    [[nodiscard]] virtual iterator<rtype<kind::int4>> begin_int4() const noexcept = 0;
    [[nodiscard]] virtual iterator<rtype<kind::int8>> begin_int8() const noexcept = 0;
    [[nodiscard]] virtual iterator<rtype<kind::float4>> begin_float4() const noexcept = 0;
    [[nodiscard]] virtual iterator<rtype<kind::float8>> begin_float8() const noexcept = 0;
    [[nodiscard]] virtual iterator<rtype<kind::character>> begin_character() const noexcept = 0;

    /**
     * @brief getter of end iterator
     * @return iterator at the end of the store
     * @warning the returned iterator will be invalid when new append() is called
     */
    [[nodiscard]] virtual iterator<rtype<kind::int4>> end_int4() const noexcept = 0;
    [[nodiscard]] virtual iterator<rtype<kind::int8>> end_int8() const noexcept = 0;
    [[nodiscard]] virtual iterator<rtype<kind::float4>> end_float4() const noexcept = 0;
    [[nodiscard]] virtual iterator<rtype<kind::float8>> end_float8() const noexcept = 0;
    [[nodiscard]] virtual iterator<rtype<kind::character>> end_character() const noexcept = 0;

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
     */
    typed_value_store(
        memory::paged_memory_resource* record_resource,
        memory::paged_memory_resource* varlen_resource
    ) :
        resource_(record_resource),
        varlen_resource_(varlen_resource)
    {}

    /**
     * @brief copy and store the value
     * For varlen data such as text, the data on the varlen buffer will be copied using varlen resource assigned to
     * this object unless it's nullptr.
     * @param record source of the record added to this container
     * @return record ref to the stored record
     */
    void append_int4(rtype<kind::int4> value) override {
        if constexpr (std::is_same_v<T, rtype<kind::int4>>) { //NOLINT
            append(&value);
        }
    }

    void append_int8(rtype<kind::int8> value) override {
        if constexpr (std::is_same_v<T, rtype<kind::int8>>) { //NOLINT
            append(&value);
        }
    }

    void append_float4(rtype<kind::float4> value) override {
        if constexpr (std::is_same_v<T, rtype<kind::float4>>) { //NOLINT
            append(&value);
        }
    }

    void append_float8(rtype<kind::float8> value) override {
        if constexpr (std::is_same_v<T, rtype<kind::float8>>) { //NOLINT
            append(&value);
        }
    }

    void append_character(rtype<kind::character> value) override {
        if constexpr (std::is_same_v<T, rtype<kind::character>>) { //NOLINT
            append(&value);
        }
    }
    /**
     * @copydoc record_store::count()
     */
    [[nodiscard]] std::size_t count() const noexcept override {
        return count_;
    }

    /**
     * @copydoc record_store::empty()
     */
    [[nodiscard]] bool empty() const noexcept override {
        return count_ == 0;
    }

    /**
     * @brief getter of begin iterator
     * @return iterator at the beginning of the store
     * @warning the returned iterator will be invalid when new append() is called.
     */
    [[nodiscard]] iterator<rtype<kind::int4>> begin_int4() const noexcept override {
        if constexpr (std::is_same_v<T, rtype<kind::int4>>) { //NOLINT
            return iterator<T>{ranges_, ranges_.begin()};
        } else { //NOLINT
            return {};
        }
    }

    [[nodiscard]] iterator<rtype<kind::int8>> begin_int8() const noexcept override {
        if constexpr (std::is_same_v<T, rtype<kind::int8>>) {  //NOLINT
            return iterator<T>{ranges_, ranges_.begin()};
        } else {  //NOLINT
            return {};
        }
    }

    [[nodiscard]] iterator<rtype<kind::float4>> begin_float4() const noexcept override {
        if constexpr (std::is_same_v<T, rtype<kind::float4>>) {  //NOLINT
            return iterator<T>{ranges_, ranges_.begin()};
        } else {  //NOLINT
            return {};
        }
    }

    [[nodiscard]] iterator<rtype<kind::float8>> begin_float8() const noexcept override {
        if constexpr (std::is_same_v<T, rtype<kind::float8>>) {  //NOLINT
            return iterator<T>{ranges_, ranges_.begin()};
        } else {  //NOLINT
            return {};
        }
    }

    [[nodiscard]] iterator<rtype<kind::character>> begin_character() const noexcept override {
        if constexpr (std::is_same_v<T, rtype<kind::character>>) {  //NOLINT
            return iterator<T>{ranges_, ranges_.begin()};
        } else {  //NOLINT
            return {};
        }
    }

    /**
     * @brief getter of end iterator
     * @return iterator at the end of the store
     * @warning the returned iterator will be invalid when new append() is called
     */
    [[nodiscard]] iterator<rtype<kind::int4>> end_int4() const noexcept override {
        if constexpr (std::is_same_v<T, rtype<kind::int4>>) {  //NOLINT
            return iterator<T>{ranges_, ranges_.end()};
        } else {  //NOLINT
            return {};
        }
    }

    [[nodiscard]] iterator<rtype<kind::int8>> end_int8() const noexcept override {
        if constexpr (std::is_same_v<T, rtype<kind::int8>>) {  //NOLINT
            return iterator<T>{ranges_, ranges_.end()};
        } else {  //NOLINT
            return {};
        }
    }

    [[nodiscard]] iterator<rtype<kind::float4>> end_float4() const noexcept override {
        if constexpr (std::is_same_v<T, rtype<kind::float4>>) {  //NOLINT
            return iterator<T>{ranges_, ranges_.end()};
        } else {  //NOLINT
            return {};
        }
    }

    [[nodiscard]] iterator<rtype<kind::float8>> end_float8() const noexcept override {
        if constexpr (std::is_same_v<T, rtype<kind::float8>>) {  //NOLINT
            return iterator<T>{ranges_, ranges_.end()};
        } else {  //NOLINT
            return {};
        }
    }

    [[nodiscard]] iterator<rtype<kind::character>> end_character() const noexcept override {
        if constexpr (std::is_same_v<T, rtype<kind::character>>) {  //NOLINT
            return iterator<T>{ranges_, ranges_.end()};
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
        prev_ = nullptr;
        ranges_.clear();
    }

private:
    memory::paged_memory_resource* resource_{};
    memory::paged_memory_resource* varlen_resource_{};
    std::size_t count_{};
    value_pointer prev_{};
    range_list ranges_{};

    void append(void* src) {
        auto* p = static_cast<value_pointer>(resource_->allocate(value_length, value_alignment));
        if (!p) std::abort();
        if constexpr (std::is_same_v<T, accessor::text>) {  //NOLINT
            BOOST_ASSERT(varlen_resource_ != nullptr);  //NOLINT
            accessor::text t{varlen_resource_, *reinterpret_cast<accessor::text*>(src)}; //NOLINT
            std::memcpy(p, &t, value_length);
        } else {  //NOLINT
            std::memcpy(p, src, value_length);
        }
        ++count_;

        if (prev_ == nullptr || p != prev_ + 1) { //NOLINT
            // starting new range
            ranges_.emplace_back(p, nullptr);
        }
        ranges_.back().e_ = p + 1; //NOLINT
        prev_ = p;
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
     */
    value_store(
        meta::field_type const& type,
        memory::paged_memory_resource* resource,
        memory::paged_memory_resource* varlen_resource
    ) :
        type_(type),
        base_(make_typed_store(type, resource, varlen_resource))
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
        if constexpr(std::is_same_v<T, rtype<kind::int4>>) {  //NOLINT
            base_->append_int4(value);
        } else if constexpr(std::is_same_v<T, rtype<kind::int8>>) {  //NOLINT
            base_->append_int8(value);
        } else if constexpr(std::is_same_v<T, rtype<kind::float4>>) {  //NOLINT
            base_->append_float4(value);
        } else if constexpr(std::is_same_v<T, rtype<kind::float8>>) {  //NOLINT
            base_->append_float8(value);
        } else if constexpr(std::is_same_v<T, rtype<kind::character>>) {  //NOLINT
            base_->append_character(value);
        } else {
            fail();
        }
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
    [[nodiscard]] details::iterator<T> begin() const noexcept {
        if constexpr(std::is_same_v<T, rtype<kind::int4>>) {  //NOLINT
            return base_->begin_int4();
        } else if constexpr(std::is_same_v<T, rtype<kind::int8>>) {  //NOLINT
            return base_->begin_int8();
        } else if constexpr(std::is_same_v<T, rtype<kind::float4>>) {  //NOLINT
            return base_->begin_float4();
        } else if constexpr(std::is_same_v<T, rtype<kind::float8>>) {  //NOLINT
            return base_->begin_float8();
        } else if constexpr(std::is_same_v<T, rtype<kind::character>>) {  //NOLINT
            return base_->begin_character();
        } else {
            fail();
        }
    }

    /**
     * @brief getter of end iterator
     * @return iterator at the end of the store
     * @warning the returned iterator will be invalid when new append() is called
     */
    template <class T>
    [[nodiscard]] details::iterator<T> end() const noexcept {
        if constexpr(std::is_same_v<T, rtype<kind::int4>>) {  //NOLINT
            return base_->end_int4();
        } else if constexpr(std::is_same_v<T, rtype<kind::int8>>) {  //NOLINT
            return base_->end_int8();
        } else if constexpr(std::is_same_v<T, rtype<kind::float4>>) {  //NOLINT
            return base_->end_float4();
        } else if constexpr(std::is_same_v<T, rtype<kind::float8>>) {  //NOLINT
            return base_->end_float8();
        } else if constexpr(std::is_same_v<T, rtype<kind::character>>) {  //NOLINT
            return base_->end_character();
        } else {
            fail();
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
        memory::paged_memory_resource* varlen_resource
    ) {
        switch(type.kind()) {
            case kind::int4: return std::make_unique<details::typed_value_store<rtype<kind::int4>>>(record_resource, varlen_resource);
            case kind::int8: return std::make_unique<details::typed_value_store<rtype<kind::int8>>>(record_resource, varlen_resource);
            case kind::float4: return std::make_unique<details::typed_value_store<rtype<kind::float4>>>(record_resource, varlen_resource);
            case kind::float8: return std::make_unique<details::typed_value_store<rtype<kind::float8>>>(record_resource, varlen_resource);
            case kind::character: return std::make_unique<details::typed_value_store<rtype<kind::character>>>(record_resource, varlen_resource);
            default:
                fail();
        }
    }
};

} // namespace
