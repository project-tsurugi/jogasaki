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

#include <tuple>
#include <sstream>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_traits.h>
#include <jogasaki/executor/exchange/group/shuffle_info.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/accessor/record_printer.h>
#include <jogasaki/utils/interference_size.h>

namespace jogasaki::mock {

using kind = meta::field_type_kind;
using takatori::util::maybe_shared_ptr;
using takatori::util::fail;

template<kind Kind>
struct to_runtime_type {
    using type = typename meta::field_type_traits<Kind>::runtime_type;
};

template<kind Kind>
using to_runtime_type_t = typename to_runtime_type<Kind>::type;

template <size_t I, kind ...Kinds>
std::size_t offset_at(std::tuple<to_runtime_type_t<Kinds>...>& entity) {
    return static_cast<char*>(static_cast<void*>(std::addressof(std::get<I>(entity)))) - static_cast<char*>(static_cast<void*>(std::addressof(entity)));
}

template <kind ...Kinds, size_t ... Is>
std::vector<std::size_t> offsets(std::tuple<to_runtime_type_t<Kinds>...>& entity, std::index_sequence<Is...>) {
    return {offset_at<Is, Kinds...>(entity)...};
}

template <kind ...Kinds>
std::vector<std::size_t> offsets(std::tuple<to_runtime_type_t<Kinds>...>& entity) {
    return offsets<Kinds...>(entity, std::make_index_sequence<sizeof...(Kinds)>());
}

template <kind Kind>
to_runtime_type_t<Kind> value(to_runtime_type_t<Kind> arg, memory::paged_memory_resource*) {
    return arg;
}

template <>
inline to_runtime_type_t<kind::character> value<kind::character>(to_runtime_type_t<kind::character> arg, memory::paged_memory_resource* resource) {
    if (resource != nullptr) {
        return accessor::text{resource, static_cast<std::string_view>(arg)};
    }
    return arg;
}

template <kind ...Kinds, size_t ... Is>
std::tuple<to_runtime_type_t<Kinds>...> values(accessor::record_ref ref, meta::record_meta& meta, memory::paged_memory_resource* resource, std::index_sequence<Is...>) {
    return std::tuple<to_runtime_type_t<Kinds>...>{ value<Kinds>(ref.get_value<to_runtime_type_t<Kinds>>(meta.value_offset(Is)), resource)...};
}

template <kind ...Kinds>
std::tuple<to_runtime_type_t<Kinds>...> values(accessor::record_ref ref, memory::paged_memory_resource* resource, meta::record_meta& meta) {
    return values<Kinds...>(ref, meta, resource, std::make_index_sequence<sizeof...(Kinds)>());
}

/**
 * @brief record object for testing
 * @tparam Kinds list of field kind for the record fields
 * @details this object represents a handy record instance, and provides convenient way to materialize record that can
 * be stored in the C++ containers. Also, meta data can be defined based on the field types, or passed from outside.
 */
template<kind ...Kinds>
class basic_record {
public:
    using entity_type = std::tuple<to_runtime_type_t<Kinds>...>;

    /**
     * @brief create empty object - only meta data is meaningful
     */
    basic_record() : meta_(create_meta(
        boost::dynamic_bitset<std::uint64_t>{sizeof...(Kinds)},  // all fields non-nullable
        std::vector<std::size_t>{(void(Kinds), 0)...}
        )){}

    /**
     * @brief construct new object with non-nullable fields
     * @warning new record_meta is created based on the template parameter. This constructor should not be used
     * when creating large number of (e.g. thousands of) records.
     */
    template <typename T = std::enable_if<sizeof...(Kinds) != 0, void>>
    explicit basic_record(to_runtime_type_t<Kinds>...args) :
        entity_(args...),
        meta_(create_meta(
            boost::dynamic_bitset<std::uint64_t>{sizeof...(args)},  // all fields non-nullable
            std::vector<std::size_t>{(void(args), 0)...}
        ))
    {}

    /**
     * @brief construct new object with given meta data and field values
     * @param meta the meta data for sharing among multiple basic_record instances (this must be compatible with
     * the underlying entity's memory layout)
     */
    template <typename T = std::enable_if<sizeof...(Kinds) != 0, void>>
    explicit basic_record(
        maybe_shared_ptr<meta::record_meta> meta,
        to_runtime_type_t<Kinds>...args
    ) :
        entity_(args...),
        meta_(std::move(meta))
    {}

    /**
     * @brief construct new object with given nullability offsets
     * @param nullability bitset that represents if each field is nullable or not
     * @param nullity_offset_table bit offset table that indicates the nullity offset of each nullable field
     * @warning new record_meta is created based on the template parameter. This constructor should not be used
     * when creating large number of (e.g. thousands of) records.
     */
    basic_record(
        boost::dynamic_bitset<std::uint64_t> nullability,
        std::vector<std::size_t> nullity_offset_table,
        to_runtime_type_t<Kinds>...args
    ) :
        entity_(args...),
        meta_(create_meta(std::move(nullability), std::move(nullity_offset_table)))
    {}

    /**
     * @brief construct new object from record_ref with default meta data
     * @param ref the record_ref whose values are copied to new object
     * @warning new record_meta is created based on the template parameter. This constructor should not be used
     * when creating large number of (e.g. thousands of) records.
     */
    explicit basic_record(accessor::record_ref ref, memory::paged_memory_resource* resource = nullptr) : basic_record() {
        entity_ = values<Kinds...>(ref, resource, *meta_);
    }

    /**
     * @brief construct empty object with given meta data
     * @param meta the meta data for sharing among multiple basic_record instances (this must be compatible with
     * the underlying entity's memory layout)
     */
    explicit basic_record(maybe_shared_ptr<meta::record_meta> meta) noexcept : meta_(std::move(meta)) {}

    /**
     * @brief construct new object from record_ref with given meta data
     * @param ref the record_ref whose values are copied to new object
     * @param meta the meta data for sharing among multiple basic_record instances (this must be compatible with
     * the underlying entity's memory layout)
     */
    basic_record(accessor::record_ref ref, maybe_shared_ptr<meta::record_meta> meta, memory::paged_memory_resource* resource = nullptr) : basic_record(std::move(meta)) {  // NOLINT(performance-unnecessary-value-param)
        entity_ = values<Kinds...>(ref, resource, *meta_);
    }

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
        return accessor::record_ref(const_cast<entity_type*>(std::addressof(entity_)), sizeof(entity_)); //FIXME use of const_cast
    }

    /// @brief equality comparison operator
    friend bool operator==(basic_record<Kinds...> const& a, basic_record<Kinds...> const& b) noexcept {
        return a.entity_ == b.entity_;
    }

    /// @brief inequality comparison operator
    friend bool operator!=(basic_record<Kinds...> const& a, basic_record<Kinds...> const& b) noexcept {
        return !(a == b);
    }

    /**
     * @brief appends string representation of the given value.
     * @param out the target output
     * @param value the target value
     * @return the output
     */
    friend std::ostream& operator<<(std::ostream& out, basic_record<Kinds...> const& value) {
        auto& v = const_cast<basic_record<Kinds...>&>(value);
        std::stringstream ss{};
        ss << v.ref() << *v.meta_;
        return out << ss.str();
    }

protected:
    [[nodiscard]] entity_type& entity() noexcept {
        return entity_;
    }

    [[nodiscard]] entity_type const& entity() const noexcept {
        return entity_;
    }

private:
    entity_type entity_{};
    takatori::util::maybe_shared_ptr<meta::record_meta> meta_{};

    template <typename T = std::enable_if<sizeof...(Kinds) != 0, void>>
    std::shared_ptr<meta::record_meta> create_meta(
        boost::dynamic_bitset<std::uint64_t> nullability,
        std::vector<std::size_t> nullity_offset_table
        ) {
        // too many creation is likely to be a program error (e.g. using wrong constructor)
        static constexpr std::size_t limit_creating_meta = 1000;
        cache_align thread_local std::size_t create_count = 0;
        if (++create_count > limit_creating_meta) {
            fail();
        }
        return std::make_shared<meta::record_meta>(
            std::vector<meta::field_type>{meta::field_type(takatori::util::enum_tag<Kinds>)...},
            std::move(nullability),
            std::vector<std::size_t>{offsets<Kinds...>(entity_)},
            std::move(nullity_offset_table),
            alignof(std::tuple<to_runtime_type_t<Kinds>...>),
            sizeof(entity_)
        );
    }
};


class record : public basic_record<kind::int8, kind::float8> {
public:
    using key_type = std::int64_t;
    using value_type = double;

    record() noexcept : basic_record(0, 0.0) {};

    record(key_type key, value_type value) : basic_record(key, value) {}

    [[nodiscard]] key_type const& key() const noexcept {
        return std::get<0>(entity());
    }

    void key(key_type arg) noexcept {
        std::get<0>(entity()) = arg;
    }

    [[nodiscard]] value_type const& value() const noexcept {
        return std::get<1>(entity());
    }

    void value(value_type arg) noexcept {
        std::get<1>(entity()) = arg;
    }
};

static_assert(sizeof(record) == 40);
static_assert(alignof(record) == 8);
static_assert(!std::is_trivially_copyable_v<record>);

class record_f4f8ch : public basic_record<kind::float8, kind::int4, kind::character> {
public:
    using key_type = std::int32_t;
    using f4_value_type = double;
    using ch_value_type = meta::field_type_traits<kind::character>::runtime_type;

    record_f4f8ch() noexcept : basic_record(0.0, 0, ch_value_type{}) {};

    record_f4f8ch(f4_value_type f4_value, key_type key, ch_value_type ch_value) : basic_record(f4_value, key, ch_value) {}

    [[nodiscard]] key_type const& key() const noexcept {
        return std::get<1>(entity());
    }

    [[nodiscard]] f4_value_type const& f4_value() const noexcept {
        return std::get<0>(entity());
    }

    [[nodiscard]] ch_value_type const& ch_value() const noexcept {
        return std::get<2>(entity());
    }
};

}
