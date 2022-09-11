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

#include <cstddef>
#include <type_traits>
#include <variant>

#include <takatori/util/comparable_traits.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/meta/field_type_traits.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_option.h>

namespace jogasaki::meta {

// gcc7 confuses when enum_tag_t is used with different kind, e.g. field_type_kind and event_kind,
// and raises compile error.
// For workaround, define local enum_tag_t like struct and use it for field_type constructor to avoid type conflict.
// The problem doesn't happen on gcc 8 or newer. When moving to new gcc, remove these tags and recover
// use of original enum_tag_t.
template<auto Kind>
struct field_enum_tag_t {
    explicit field_enum_tag_t() = default;
};
template<auto Kind>
inline constexpr field_enum_tag_t<Kind> field_enum_tag {};

/**
 * @brief type information for a field
 * @details this class owns type kind, which is sufficient information for simple types, and optional
 * type information for complex types.
 * It is designed to be copyable and movable so that it can be passed around without using smart pointers.
 * Optional type information can be retrieved by option() in a type-safely manner.
 */
class field_type final {
public:

    /**
     * @brief entity type for the field information
     */
    using entity_type = std::variant<
        std::monostate, // undefined
        std::monostate, // boolean
        std::monostate, // int1
        std::monostate, // int2
        std::monostate, // int4
        std::monostate, // int8
        std::monostate, // float4
        std::monostate, // float8
        std::shared_ptr<decimal_field_option>, // decimal
        std::monostate, // character
        std::monostate, // bit
        std::monostate, // date
        std::shared_ptr<time_of_day_field_option>, // time_of_day
        std::shared_ptr<time_point_field_option>,
        std::monostate, // time_interval
        std::shared_ptr<array_field_option>,
        std::shared_ptr<record_field_option>,
        std::shared_ptr<unknown_field_option>,
        std::shared_ptr<row_reference_field_option>,
        std::shared_ptr<row_id_field_option>,
        std::shared_ptr<declared_field_option>,
        std::shared_ptr<extension_field_option>,
        std::monostate, // reference_column_position (internal use)
        std::monostate, // reference_column_name (internal use)
        std::monostate // pointer (internal use)
    >;

    /**
     * @brief the option type for each kind.
     * @tparam kind the element kind
     */
    template<field_type_kind Kind>
    using option_type = std::variant_alternative_t<
            static_cast<std::size_t>(Kind),
            entity_type>;

    /**
     * @brief construct empty object (kind undefined)
     */
    constexpr field_type() noexcept = default;

    /**
     * @brief construct new object
     * @tparam Kind type kind for new object
     */
    template <field_type_kind Kind>
    explicit field_type(field_enum_tag_t<Kind>) noexcept :
        entity_(std::in_place_index<static_cast<std::size_t>(Kind)>)
    {
        static_assert(std::is_same_v<
            std::variant_alternative_t<static_cast<std::size_t>(Kind), entity_type>,
            std::monostate
        >);
    }

    /**
     * @brief construct new field with optional information
     * @param option field option
     */
    template <class T>
    explicit field_type(std::shared_ptr<T> option) noexcept :
            entity_(std::in_place_type<std::shared_ptr<T>>, std::move(option)) {}

    /**
     * @brief getter for type kind
     */
    [[nodiscard]] constexpr field_type_kind kind() const noexcept {
        auto index = entity_.index();
        if (index == std::variant_npos) {
            std::abort();
        }
        return static_cast<field_type_kind>(index);
    }

    /**
     * @brief getter for option information
     * @throws std::bad_variant_access if specified kind and stored one are incompatible
     */
    template <field_type_kind Kind>
    [[nodiscard]] constexpr option_type<Kind> const& option() const {
        return std::get<static_cast<std::size_t>(Kind)>(entity_);
    }

    /**
     * @brief getter for option information without checking field type kind being consistent with content
     */
    template <field_type_kind Kind>
    [[nodiscard]] option_type<Kind> const& option_unsafe() const noexcept {
        try {
            return option<Kind>();
        } catch (std::bad_variant_access&) {
            std::abort();
        }
    }

    /**
     * @return true if field type is valid
     * @return false otherwise
     */
    [[nodiscard]] explicit constexpr operator bool() const noexcept {
        return kind() != field_type_kind::undefined;
    }

    /**
     * @brief retrieve runtime type size in bytes used to represent binary of this field
     * @return bytes used by the runtime type of this field
     */
    [[nodiscard]] constexpr std::size_t runtime_type_size() const noexcept {
        using k = field_type_kind;
        switch (kind()) {
            case k::boolean: return field_type_traits<k::boolean>::size;
            case k::int1: return field_type_traits<k::int1>::size;
            case k::int2: return field_type_traits<k::int2>::size;
            case k::int4: return field_type_traits<k::int4>::size;
            case k::int8: return field_type_traits<k::int8>::size;
            case k::float4: return field_type_traits<k::float4>::size;
            case k::float8: return field_type_traits<k::float8>::size;
            case k::decimal: return field_type_traits<k::decimal>::size;
            case k::character: return field_type_traits<k::character>::size;
            case k::date: return field_type_traits<k::date>::size;
            case k::time_of_day: return field_type_traits<k::time_of_day>::size;
            case k::time_point: return field_type_traits<k::time_point>::size;
            case k::reference_column_position: std::abort(); // should not be used as runtime type
            case k::reference_column_name: std::abort(); // should not be used as runtime type
            case k::pointer: return field_type_traits<k::pointer>::size;
            case k::unknown: return field_type_traits<k::unknown>::size;
            default:
                // TODO implement cases for complex types
                std::abort();
        }
    }

    /**
     * @brief retrieve alignment in bytes used for the runtime type of this field
     * @return alignment used by the runtime type of this field
     */
    [[nodiscard]] constexpr std::size_t runtime_type_alignment() const noexcept {
        using k = field_type_kind;
        switch (kind()) {
            case k::boolean: return field_type_traits<k::boolean>::alignment;
            case k::int1: return field_type_traits<k::int1>::alignment;
            case k::int2: return field_type_traits<k::int2>::alignment;
            case k::int4: return field_type_traits<k::int4>::alignment;
            case k::int8: return field_type_traits<k::int8>::alignment;
            case k::float4: return field_type_traits<k::float4>::alignment;
            case k::float8: return field_type_traits<k::float8>::alignment;
            case k::decimal: return field_type_traits<k::decimal>::alignment;
            case k::character: return field_type_traits<k::character>::alignment;
            case k::date: return field_type_traits<k::date>::alignment;
            case k::time_of_day: return field_type_traits<k::time_of_day>::alignment;
            case k::time_point: return field_type_traits<k::time_point>::alignment;
            case k::reference_column_position: std::abort(); // should not be used as runtime type
            case k::reference_column_name: std::abort(); // should not be used as runtime type
            case k::pointer: return field_type_traits<k::pointer>::alignment;
            case k::unknown: return field_type_traits<k::unknown>::alignment;
            default:
                // TODO implement cases for complex types
                std::abort();
        }
    }

    /**
     * @brief appends string representation of the given value.
     * @param out the target output
     * @param value the target value
     * @return the output stream
     */
    friend std::ostream& operator<<(std::ostream& out, field_type const& value) {
        using kind = field_type_kind;
        auto k = value.kind();
        switch (k) {
            case kind::time_of_day: return out << value.option<kind::time_of_day>();
            case kind::time_point: return out << value.option<kind::time_point>();
            case kind::array: return out << value.option<kind::array>();
            case kind::record: return out << value.option<kind::record>();
            case kind::unknown: return out << value.option<kind::unknown>();
            case kind::row_reference: return out << value.option<kind::row_reference>();
            case kind::row_id: return out << value.option<kind::row_id>();
            case kind::declared: return out << value.option<kind::declared>();
            case kind::extension: return out << value.option<kind::extension>();
            default:
                return out << k;
        }
        std::abort();
    }

private:
    entity_type entity_{};
};

namespace impl {

template <field_type_kind Kind, class = void>
struct eq {
    [[nodiscard]] bool operator()(field_type const& a, field_type const& b) const noexcept {
        auto&& r1 = a.option_unsafe<Kind>();
        auto&& r2 = b.option_unsafe<Kind>();
        return r1 == r2;
    }
};

template <field_type_kind Kind>
struct eq<
        Kind,
        std::enable_if_t<
                takatori::util::is_equal_comparable_v<
                        decltype(*std::declval<field_type::option_type<Kind>>()),
                        decltype(*std::declval<field_type::option_type<Kind>>())>>> {
    [[nodiscard]] bool operator()(field_type const& a, field_type const& b) const noexcept {
        auto&& r1 = a.option_unsafe<Kind>();
        auto&& r2 = b.option_unsafe<Kind>();
        return *r1 == *r2;
    }
};

}
/**
 * @brief equality comparison operator
 */
inline bool operator==(field_type const& a, field_type const& b) noexcept {
    if (a.kind() != b.kind()) {
        return false;
    }
    using kind = field_type_kind;
    switch (a.kind()) {
        case kind::date: return impl::eq<kind::date>()(a, b);
        case kind::time_point: return impl::eq<kind::time_point>()(a, b);
        case kind::array: return impl::eq<kind::array>()(a, b);
        case kind::record: return impl::eq<kind::record>()(a, b);
        case kind::unknown: return impl::eq<kind::unknown>()(a, b);
        case kind::row_reference: return impl::eq<kind::row_reference>()(a, b);
        case kind::row_id: return impl::eq<kind::row_id>()(a, b);
        case kind::declared: return impl::eq<kind::declared>()(a, b);
        case kind::extension: return impl::eq<kind::extension>()(a, b);
        case kind::reference_column_name: return true; // internal fields are ignored on comparison
        case kind::reference_column_position: return true; // internal fields are ignored on comparison
        case kind::pointer: return true; // internal fields are ignored on comparison
        default:
            return true;
    }
}

/**
 * @brief inequality comparison operator
 */
inline bool operator!=(field_type const& a, field_type const& b) noexcept {
    return !(a == b);
}

static_assert(std::is_copy_constructible_v<field_type>);
static_assert(std::is_move_constructible_v<field_type>);

} // namespace

