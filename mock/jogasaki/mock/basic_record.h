/*
 * Copyright 2018-2023 Project Tsurugi.
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
#include <cstring>
#include <initializer_list>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>
#include <boost/assert.hpp>
#include <boost/cstdint.hpp>
#include <boost/dynamic_bitset/dynamic_bitset.hpp>

#include <takatori/util/fail.h>
#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/accessor/record_printer.h>
#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/accessor/text.h>
#include <jogasaki/constants.h>
#include <jogasaki/data/aligned_buffer.h>
#include <jogasaki/executor/comparator.h>
#include <jogasaki/executor/compare_info.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/meta/character_field_option.h>
#include <jogasaki/meta/decimal_field_option.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_traits.h>
#include <jogasaki/meta/octet_field_option.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/meta/time_of_day_field_option.h>
#include <jogasaki/meta/time_point_field_option.h>
#include <jogasaki/utils/interference_size.h>

namespace jogasaki::mock {

using kind = meta::field_type_kind;
using takatori::util::maybe_shared_ptr;
using takatori::util::fail;

constexpr static std::size_t basic_record_field_size = 32;
constexpr static std::size_t basic_record_field_alignment = 8;
constexpr static std::size_t basic_record_max_field_count = 100;
constexpr static std::size_t basic_record_buffer_size =
    basic_record_field_size * (basic_record_max_field_count + 1); // +1 for nullity bits
using basic_record_entity_type = std::array<char, basic_record_buffer_size>;

namespace details {

template <size_t Is, kind ...Kinds>
std::size_t offset_at() {
    return Is*basic_record_field_size;
}

template <kind ...Kinds, size_t ... Is>
std::vector<std::size_t> offsets(std::index_sequence<Is...>) {
    return {offset_at<Is, Kinds...>()...};
}

template <kind ...Kinds>
std::vector<std::size_t> offsets() {
    return offsets<Kinds...>(std::make_index_sequence<sizeof...(Kinds)>());
}

template <size_t Is, kind Kind>
void create_entity(basic_record_entity_type& entity, runtime_t<Kind> arg) {
    std::memset(&entity[0]+Is*basic_record_field_size, 0, basic_record_field_size);  //NOLINT
    std::memcpy(&entity[0]+Is*basic_record_field_size, &arg, sizeof(arg)); //NOLINT
}

template <kind ...Kinds, size_t ... Is>
void create_entity(
    basic_record_entity_type& entity,
    memory::paged_memory_resource* resource,
    std::index_sequence<Is...>,
    meta::record_meta& meta,
    runtime_t<Kinds>...args
) {
    (void)resource;
    (void)meta;
    [](auto&&...){}((create_entity<Is, Kinds>(entity, args), 0)...);
}

template <kind ...Kinds>
void create_entity(
    basic_record_entity_type& entity,
    memory::paged_memory_resource* resource,
    meta::record_meta& meta,
    runtime_t<Kinds>...args
) {
    create_entity<Kinds...>(entity, resource, std::make_index_sequence<sizeof...(Kinds)>(), meta, args...);
}

inline void create_entity(
    basic_record_entity_type& entity,
    accessor::record_ref record,
    memory::paged_memory_resource* resource,
    meta::record_meta& meta
) {
    std::memset(&entity[0], 0, sizeof(entity));
    std::memcpy(&entity[0], record.data(), meta.record_size());
    if (resource != nullptr) {
        std::size_t i=0;
        for(auto&& f : meta) {
            if (f.kind() == kind::character) {
                if (!meta.nullable(i) || !record.is_null(meta.nullity_offset(i))) {
                    accessor::text copy(resource, record.get_value<accessor::text>(meta.value_offset(i)));
                    std::memcpy(&entity[0]+i*basic_record_field_size, &copy, sizeof(accessor::text));  //NOLINT
                }
            } else if (f.kind() == kind::octet) {
                if (!meta.nullable(i) || !record.is_null(meta.nullity_offset(i))) {
                    accessor::binary copy(resource, record.get_value<accessor::binary>(meta.value_offset(i)));
                    std::memcpy(&entity[0]+i*basic_record_field_size, &copy, sizeof(accessor::binary));  //NOLINT
                }
            }
            ++i;
        }
    }
}

template <std::size_t ...Is>
std::vector<std::size_t> index_vector(std::size_t init, std::index_sequence<Is...>) {
    return std::vector<std::size_t>{(init + Is)...};
}

template <meta::field_type_kind Kind>
meta::field_type create_field_type() {
    return meta::field_type(meta::field_enum_tag<Kind>);
}

template <>
inline meta::field_type create_field_type<meta::field_type_kind::time_of_day>() {
    return meta::field_type(std::make_shared<meta::time_of_day_field_option>());
}

template <>
inline meta::field_type create_field_type<meta::field_type_kind::time_point>() {
    return meta::field_type(std::make_shared<meta::time_point_field_option>());
}

template <>
inline meta::field_type create_field_type<meta::field_type_kind::decimal>() {
    return meta::field_type(std::make_shared<meta::decimal_field_option>());
}

template <>
inline meta::field_type create_field_type<meta::field_type_kind::character>() {
    return meta::field_type(std::make_shared<meta::character_field_option>());
}

template <>
inline meta::field_type create_field_type<meta::field_type_kind::octet>() {
    return meta::field_type(std::make_shared<meta::octet_field_option>());
}

}  //namespace details

template <kind ...Kinds, typename = std::enable_if_t<sizeof...(Kinds) != 0>>
std::shared_ptr<meta::record_meta> create_meta(
    std::vector<meta::field_type> types,
    boost::dynamic_bitset<std::uint64_t> nullability,
    bool all_fields_nullable = false
) {
    (void)all_fields_nullable;  // for now nulliti bits always exist
    static_assert(sizeof...(Kinds) <= basic_record_max_field_count);
    static_assert(sizeof...(Kinds) <= basic_record_field_size * bits_per_byte); // nullity bits should be
                                                                                // contained in a field
    // too many creation is likely to be a program error (e.g. using wrong constructor)
    static constexpr std::size_t limit_creating_meta = 1000000;
    cache_align thread_local std::size_t create_count = 0;
    if (++create_count > limit_creating_meta) {
        fail();
    }

    std::vector<std::size_t> offsets{details::offsets<Kinds...>()};
    auto nullity_offset_base = (offsets.back()+basic_record_field_size)*bits_per_byte;
    return std::make_shared<meta::record_meta>(
        std::move(types),
        std::move(nullability),
        std::move(offsets),
        details::index_vector(nullity_offset_base, std::make_index_sequence<sizeof...(Kinds)>()),
        basic_record_field_alignment,
        (sizeof...(Kinds) + 1) * basic_record_field_size  // +1 for nullity bits at the bottom
    );
}

template <kind ...Kinds, typename = std::enable_if_t<sizeof...(Kinds) != 0>>
std::shared_ptr<meta::record_meta> create_meta(
    boost::dynamic_bitset<std::uint64_t> nullability,
    bool all_fields_nullable = false
) {
    std::vector<meta::field_type> types{details::create_field_type<Kinds>()...};
    return create_meta<Kinds...>(std::move(types), std::move(nullability), all_fields_nullable);
}

template <kind ...Kinds, typename = std::enable_if_t<sizeof...(Kinds) != 0>>
std::shared_ptr<meta::record_meta> create_meta(bool all_fields_nullable = false) {
    boost::dynamic_bitset<std::uint64_t> bitset{sizeof...(Kinds)};
    if (all_fields_nullable) {
        bitset.flip();
    }
    return create_meta<Kinds...>(
        bitset
    );
}

template <class Tuple,
    class T = std::decay_t<std::tuple_element_t<0, std::decay_t<Tuple>>>>
std::vector<T> to_vector(Tuple&& tuple)
{
    return std::apply([](auto&&... elems){
        return std::vector<T>{std::forward<decltype(elems)>(elems)...};
    }, std::forward<Tuple>(tuple));
}

template <
    kind ...Kinds,
    class...Args,
    typename = std::enable_if_t<sizeof...(Args) !=0>,
    typename = std::enable_if_t<sizeof...(Kinds) !=0>,
    typename = std::enable_if_t<sizeof...(Kinds) == sizeof...(Args)>,
    typename = typename std::enable_if<(true && ... && std::is_same_v<Args, meta::field_type>), void>::type
>
std::shared_ptr<meta::record_meta> typed_meta(
    bool all_fields_nullable,
    std::tuple<Args...> types
) {
    boost::dynamic_bitset<std::uint64_t> bitset{sizeof...(Kinds)};
    if (all_fields_nullable) {
        bitset.flip();
    }
    return create_meta<Kinds...>(to_vector(types), std::move(bitset), all_fields_nullable);
}

/**
 * @brief record object for testing
 * @details this object represents a handy record instance, and provides convenient way to materialize record that can
 * be stored in the C++ containers. Also, meta data can be defined based on the field types, or passed from outside.
 */
class basic_record {
public:
    constexpr static std::size_t buffer_size = basic_record_buffer_size;

    using entity_type = basic_record_entity_type ;
    /**
     * @brief create empty object
     */
    basic_record() = default;

    ~basic_record() = default;

    basic_record(basic_record const& other) :
        meta_(other.meta_),
        entity_(other.entity_),
        varlen_fields_(other.varlen_fields_.size())
    {
        for(std::size_t i=0, n=varlen_fields_.size(); i<n; ++i) {
            varlen_fields_[i].assign(other.varlen_fields_[i]);
        }
        // TODO correct varlen field reference from accessor::text fields
    }

    basic_record& operator=(basic_record const& other) {
        meta_ = other.meta_;
        entity_ = other.entity_;
        varlen_fields_.resize(other.varlen_fields_.size());
        for(std::size_t i=0, n=varlen_fields_.size(); i<n; ++i) {
            varlen_fields_[i].assign(other.varlen_fields_[i]);
        }
        return *this;
    }

    basic_record(basic_record&& other) noexcept = default;
    basic_record& operator=(basic_record&& other) noexcept = default;

    /**
     * @brief create new object
     * @param metadata the stored record meta information
     */
    basic_record(
        maybe_shared_ptr<meta::record_meta> meta,
        entity_type const& src
    ) :
        meta_(std::move(meta))
    {
        std::memcpy(&entity_[0], &src[0], sizeof(entity_type));
    }

    /**
     * @brief construct empty object with given meta data
     * @param meta the meta data for sharing among multiple basic_record instances (this must be compatible with
     * the underlying entity's memory layout)
     */
    explicit basic_record(
        maybe_shared_ptr<meta::record_meta> meta
    ) noexcept :
        meta_(std::move(meta))
    {}

    /**
     * @brief construct new object from record_ref with given meta data
     * @param ref the record_ref whose values are copied to new object
     * @param meta the meta data for sharing among multiple basic_record instances (this must be compatible with
     * the underlying entity's memory layout)
     */
    basic_record(
        accessor::record_ref ref,
        maybe_shared_ptr<meta::record_meta> meta,
        memory::paged_memory_resource* resource = nullptr
    ) :
        basic_record(std::move(meta)) // NOLINT(performance-unnecessary-value-param)
    {
        details::create_entity(entity_, ref, resource, *meta_);
    }

    /**
     * @brief construct new object from pointer to record data with given meta data
     * @param src the record data area whose values are copied to new object
     * @param meta the meta data for sharing among multiple basic_record instances (this must be compatible with
     * the underlying entity's memory layout)
     */
    basic_record(
        void* src,
        maybe_shared_ptr<meta::record_meta> const& meta,
        memory::paged_memory_resource* resource = nullptr
    ) :
        basic_record(accessor::record_ref{src, meta->record_size()}, meta, resource)  // NOLINT(performance-unnecessary-value-param)
    {}

    /**
     * @brief accessor to the meta data of the record
     * @return the meta data
     */
    [[nodiscard]] maybe_shared_ptr<meta::record_meta> const& record_meta() const noexcept {
        return meta_;
    }

    /**
     * @brief accessor to the record_ref that represents this record object
     * @return record ref of the record
     */
    [[nodiscard]] accessor::record_ref ref() const noexcept {
        return accessor::record_ref(const_cast<char*>(std::addressof(entity_[0])), meta_->record_size());
    }

    /**
     * @brief returns whether the object is valid or not
     */
    [[nodiscard]] explicit operator bool() const noexcept {
        return static_cast<bool>(meta_);
    }

    /**
     * @brief returns the field value
     */
    template <class T>
    [[nodiscard]] T get_value(std::size_t field) const noexcept {
        return ref().get_value<T>(record_meta()->value_offset(field));
    }

    /**
     * @brief returns the field value or null
     */
    template <class T>
    [[nodiscard]] std::optional<T> get_if(std::size_t field) const noexcept {
        auto meta = record_meta();
        if (! meta->nullable(field)) {
            return {};
        }
        return ref().get_if<T>(
            meta->nullity_offset(field),
            meta->value_offset(field)
        );
    }

    /**
     * @brief returns if field is nullable
     */
    [[nodiscard]] bool is_nullable(std::size_t field) const noexcept {
        auto meta = record_meta();
        return meta->nullable(field);
    }

    /**
     * @brief returns the field value
     */
    [[nodiscard]] bool is_null(std::size_t field) const noexcept {
        auto meta = record_meta();
        if (! meta->nullable(field)) {
            return false;
        }
        return ref().is_null(meta->nullity_offset(field));
    }

    /// @brief equality comparison operator
    friend bool operator==(basic_record const& a, basic_record const& b) noexcept {
        if (a.meta_->field_count() != b.meta_->field_count()) {
            return false;
        }
        for(std::size_t i=0, n = a.meta_->field_count(); i < n; ++i) {
            if(a.meta_->at(i) != b.meta_->at(i)) {
                return false;
            }
        }
        executor::compare_info cm{*a.meta_, *b.meta_};
        executor::comparator comp{cm};
        return comp(a.ref(), b.ref()) == 0;
    }

    /// @brief inequality comparison operator
    friend bool operator!=(basic_record const& a, basic_record const& b) noexcept {
        return !(a == b);
    }

    /// @brief less than comparison operator
    friend bool operator<(basic_record const& a, basic_record const& b) noexcept {
        if (a.meta_->field_count() != b.meta_->field_count()) {
            return false;
        }
        for(std::size_t i=0, n = a.meta_->field_count(); i < n; ++i) {
            if(a.meta_->at(i) != b.meta_->at(i)) {
                return false;
            }
        }
        executor::compare_info cm{*a.meta_, *b.meta_};
        executor::comparator comp{cm};
        return comp(a.ref(), b.ref()) < 0;
    }

    /// @brief greater than comparison operator
    friend bool operator>(basic_record const& a, basic_record const& b) noexcept {
        if (a.meta_->field_count() != b.meta_->field_count()) {
            return false;
        }
        for(std::size_t i=0, n = a.meta_->field_count(); i < n; ++i) {
            if(a.meta_->at(i) != b.meta_->at(i)) {
                return false;
            }
        }
        executor::compare_info cm{*a.meta_, *b.meta_};
        executor::comparator comp{cm};
        return comp(a.ref(), b.ref()) > 0;
    }
    /**
     * @brief appends string representation of the given value.
     * @param out the target output
     * @param value the target value
     * @return the output
     */
    friend std::ostream& operator<<(std::ostream& out, basic_record const& value) {
        auto& v = const_cast<basic_record&>(value);
        std::stringstream ss{};
        ss << v.ref() << *v.meta_;
        return out << ss.str();
    }

    /**
     * @brief allocate new varlen field data area
     * @return string object to store the varlen data
     */
    std::string_view allocate_varlen_data(std::string_view sv) {
        return static_cast<std::string_view>(varlen_fields_.emplace_back(sv));
    }
protected:
    [[nodiscard]] entity_type& entity() noexcept {
        return entity_;
    }

    [[nodiscard]] entity_type const& entity() const noexcept {
        return entity_;
    }

    [[nodiscard]] maybe_shared_ptr<meta::record_meta> const& meta() const noexcept {
        return meta_;
    }

private:
    maybe_shared_ptr<meta::record_meta> meta_{};
    entity_type entity_{};
    std::vector<data::aligned_buffer> varlen_fields_{};
};

/**
 * @brief create empty record object - only meta data is meaningful
 */
template <kind ...Kinds, typename = std::enable_if_t<sizeof...(Kinds) != 0>>
basic_record create_record() {
    return basic_record(create_meta<Kinds...>());
}

/**
 * @brief construct new object with non-nullable fields
 * @param args values for each field
 * @warning new record_meta is created based on the template parameter. This function should not be used
 * when creating large number of (e.g. thousands of) records.
 */
template <kind ...Kinds, typename = std::enable_if_t<sizeof...(Kinds) != 0>>
basic_record create_record(runtime_t<Kinds>...args) {
    auto meta = create_meta<Kinds...>(
        boost::dynamic_bitset<std::uint64_t>{sizeof...(args)}  // all fields non-nullable
    );
    basic_record_entity_type buf{};
    details::create_entity<Kinds...>(buf, nullptr, *meta, (args)...);
    return basic_record(std::move(meta), buf);
}

/**
 * @brief construct new object with given meta data and field values
 * @param meta the meta data for sharing among multiple basic_record instances.
 * This must be compatible with the metadata generated by Kinds parameters.
 * @param args values for each field
 */
template <kind ...Kinds, typename = std::enable_if_t<sizeof...(Kinds) != 0>>
basic_record create_record(
    maybe_shared_ptr<meta::record_meta> meta,
    runtime_t<Kinds>...args
) {
    basic_record_entity_type buf{};
    details::create_entity<Kinds...>(buf, nullptr, *meta, (args)...);
    return basic_record(std::move(meta), buf);
}

/**
 * @brief construct new object with given nullability offsets
 * @param nullability bitset that represents if each field is nullable or not
 * @param args values for each field
 * @warning new record_meta is created based on the template parameter. This constructor should not be used
 * when creating large number of (e.g. thousands of) records.
 */
template <kind ...Kinds, typename = std::enable_if_t<sizeof...(Kinds) != 0>>
basic_record create_record(
    boost::dynamic_bitset<std::uint64_t> nullability,
    runtime_t<Kinds>...args
) {
    auto meta = create_meta<Kinds...>(std::move(nullability));
    basic_record_entity_type buf{};
    details::create_entity<Kinds...>(buf, nullptr, *meta, args...);
    return basic_record(std::move(meta), buf);
}

/**
 * @brief construct new object with given nullability and null flags
 * @param nullability bitset that represents if each field is nullable or not
 * @param args values for each field
 * @warning new record_meta is created based on the template parameter. This constructor should not be used
 * when creating large number of (e.g. thousands of) records.
 */
template <kind ...Kinds, typename = std::enable_if_t<sizeof...(Kinds) != 0>>
basic_record create_record(
    boost::dynamic_bitset<std::uint64_t> nullability,
    std::tuple<runtime_t<Kinds>...> args,
    std::initializer_list<bool> nullities = {}
) {
    BOOST_ASSERT(nullities.size() == 0 || nullities.size() == sizeof...(Kinds));
    BOOST_ASSERT(nullability.size() == sizeof...(Kinds));
    auto meta = create_meta<Kinds...>(nullability);
    basic_record_entity_type buf{};
    std::apply([&](runtime_t<Kinds>... values){
        details::create_entity<Kinds...>(buf, nullptr, *meta, values...);
    }, args);

    auto ret = basic_record(std::move(meta), buf);
    std::size_t i=0;
    for(auto nullity : nullities) {
        BOOST_ASSERT(!nullity || nullability[i]);  //NOLINT
        ret.ref().set_null(ret.record_meta()->nullity_offset(i), nullity);
        ++i;
    }
    return ret;
}

/**
 * @brief create empty record object - only meta data is meaningful
 */
template <kind ...Kinds, typename = std::enable_if_t<sizeof...(Kinds) != 0>>
basic_record create_nullable_record() {
    return basic_record(create_meta<Kinds...>(true));
}

/**
 * @brief construct new object with all fields nullable
 * @param args values for each field
 * @warning new record_meta is created based on the template parameter. This constructor should not be used
 * when creating large number of (e.g. thousands of) records.
 */
template <kind ...Kinds, typename = std::enable_if_t<sizeof...(Kinds) != 0>>
basic_record create_nullable_record(
    runtime_t<Kinds>...args
) {
    auto meta = create_meta<Kinds...>(true);
    basic_record_entity_type buf{};
    details::create_entity<Kinds...>(buf, nullptr, *meta, args...);
    auto ret = basic_record(std::move(meta), buf);
    for(std::size_t i=0, n=ret.record_meta()->field_count(); i < n ; ++i) {
        ret.ref().set_null(ret.record_meta()->nullity_offset(i), false);
    }
    return ret;
}

template <kind ...Kinds, typename = std::enable_if_t<sizeof...(Kinds) != 0>>
basic_record create_nullable_record(
    std::tuple<runtime_t<Kinds>...> args,
    std::initializer_list<bool> nullities = {}
) {
    BOOST_ASSERT(nullities.size() == 0 || nullities.size() == sizeof...(Kinds));
    auto meta = create_meta<Kinds...>(true);
    basic_record_entity_type buf{};
    std::apply([&](runtime_t<Kinds>... values){
        details::create_entity<Kinds...>(buf, nullptr, *meta, values...);
    }, args);

    auto ret = basic_record(std::move(meta), buf);
    std::size_t i=0;
    for(auto nullity : nullities) {
        ret.ref().set_null(ret.record_meta()->nullity_offset(i), nullity);
        ++i;
    }
    return ret;
}


/**
 * @brief construct new object with all fields nullable
 * @param args values for each field
 * @warning new record_meta is created based on the template parameter. This constructor should not be used
 * when creating large number of (e.g. thousands of) records.
 */
template <
    kind ...Kinds,
    class...Args,
    typename = std::enable_if_t<sizeof...(Args) !=0>,
    typename = std::enable_if_t<sizeof...(Kinds) !=0>,
    typename = std::enable_if_t<sizeof...(Kinds) == sizeof...(Args)>,
    typename = typename std::enable_if<(true && ... && std::is_same_v<Args, meta::field_type>), void>::type
>
basic_record typed_nullable_record(
    std::tuple<Args...> types,
    std::tuple<runtime_t<Kinds>...> args,
    std::initializer_list<bool> nullities = {}
) {
    auto meta = typed_meta<Kinds...>(true, types);
    basic_record_entity_type buf{};
    std::apply([&](runtime_t<Kinds>... values){
        details::create_entity<Kinds...>(buf, nullptr, *meta, values...);
    }, args);

    auto ret = basic_record(std::move(meta), buf);
    std::vector<bool> nulls{nullities};
    if(nulls.empty()) {
        nulls.resize(sizeof...(Args), false);
    }
    for(std::size_t i=0, n=sizeof...(Args); i<n; ++i) {
        ret.ref().set_null(ret.record_meta()->nullity_offset(i), nulls[i]);
    }
    return ret;
}

}
