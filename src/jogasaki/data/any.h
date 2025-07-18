/*
 * Copyright 2018-2025 Project Tsurugi.
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
#include <cstdint>
#include <ios>
#include <ostream>
#include <type_traits>
#include <utility>
#include <variant>
#include <glog/logging.h>

#include <takatori/datetime/date.h>
#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/time_point.h>
#include <takatori/decimal/triple.h>

#include <jogasaki/accessor/binary.h>
#include <jogasaki/accessor/text.h>
#include <jogasaki/executor/expr/error.h>
#include <jogasaki/lob/blob_reference.h>
#include <jogasaki/lob/clob_reference.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/utils/fail.h>
#include <jogasaki/utils/variant.h>

namespace jogasaki::data {

using jogasaki::executor::expr::error;

/**
 * @brief value store for any type
 */
class any {
public:
    using base_type = std::variant<
        std::monostate,
        class error,
        std::int8_t,
        std::int16_t,
        std::int32_t,
        std::int64_t,
        float,
        double,
        accessor::text,
        accessor::binary,
        takatori::decimal::triple,
        takatori::datetime::date,
        takatori::datetime::time_of_day,
        takatori::datetime::time_point,
        lob::blob_reference,
        lob::clob_reference,
        std::size_t  // for reference column position
    >;

    /**
     * @brief construct empty instance
     */
    any() = default;

    /**
     * @brief construct new instance
     */
    template<typename T, typename E = T>
    any(std::in_place_type_t<T>, E arg) : body_(std::in_place_type<T>, arg) {}

    /**
     * @brief accessor of the content value in given type
     */
    template<typename T>
    [[nodiscard]] T to() const {
        using A = std::conditional_t<std::is_same_v<T, bool>, std::int8_t, T>;
        if(auto* p = std::get_if<A>(&body_); p != nullptr) {
            return *p;
        }
        fail_with_exception();
    }

    /**
     * @brief return whether non-error value is assigned to this object
     */
    [[nodiscard]] explicit operator bool() const noexcept;

    /**
     * @brief return whether any value is assigned to this object
     */
    [[nodiscard]] bool empty() const noexcept;

    /**
     * @brief return whether any value is assigned to this object
     */
    [[nodiscard]] bool error() const noexcept;

    /**
     * @brief return whether any value is assigned to this object
     */
    [[nodiscard]] std::size_t type_index() const noexcept;

    // variant index in any - treat bool as std::int8_t
    template <class T>
    static constexpr std::size_t index =
        alternative_index<std::conditional_t<std::is_same_v<T, bool>, std::int8_t, T>, any::base_type>();

private:
    base_type body_{};
};

static_assert(std::is_trivially_copyable_v<any>);
static_assert(std::is_trivially_destructible_v<any>);
static_assert(std::alignment_of_v<any> == 8);
static_assert(sizeof(any) == 40);

// bool is the syntax sugar for std::int8_t
template<>
inline any::any(std::in_place_type_t<bool>, bool arg) : body_(std::in_place_type<std::int8_t>, arg ? 1 : 0) {}

template<>
inline any::any(std::in_place_type_t<bool>, std::int8_t arg) : body_(std::in_place_type<std::int8_t>, arg != 0 ? 1 : 0) {}

template <class T>
struct eq {
    [[nodiscard]] bool operator()(any const& a, any const& b) const noexcept {
        return a.to<T>() == b.to<T>();
    }
};

/**
 * @brief equality comparison operator
 */
inline bool operator==(any const& a, any const& b) noexcept {
    if(a.type_index() != b.type_index()) {
        return false;
    }
    if(a.empty() || b.empty()) {
        return a.empty() && b.empty();
    }
    switch (a.type_index()) {
        case any::index<class error>: return eq<class error>()(a, b);
        case any::index<std::int8_t>: return eq<std::int8_t>()(a, b);
        case any::index<std::int16_t>: return eq<std::int16_t>()(a, b);
        case any::index<std::int32_t>: return eq<std::int32_t>()(a, b);
        case any::index<std::int64_t>: return eq<std::int64_t>()(a, b);
        case any::index<float>: return eq<float>()(a, b);
        case any::index<double>: return eq<double>()(a, b);
        case any::index<accessor::text>: return eq<accessor::text>()(a, b);
        case any::index<accessor::binary>: return eq<accessor::binary>()(a, b);
        case any::index<takatori::decimal::triple>: return eq<takatori::decimal::triple>()(a, b);
        case any::index<takatori::datetime::date>: return eq<takatori::datetime::date>()(a, b);
        case any::index<takatori::datetime::time_of_day>: return eq<takatori::datetime::time_of_day>()(a, b);
        case any::index<takatori::datetime::time_point>: return eq<takatori::datetime::time_point>()(a, b);
        case any::index<lob::blob_reference>: return eq<lob::blob_reference>()(a, b);
        case any::index<lob::clob_reference>: return eq<lob::clob_reference>()(a, b);
        case any::index<std::size_t>: return eq<std::size_t>()(a, b);
        default:
            return false;
    }
}

/**
 * @brief inequality comparison operator
 */
inline bool operator!=(any const& a, any const& b) noexcept {
    return !(a == b);
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output stream
 */
inline std::ostream& operator<<(std::ostream& out, any const& value) {
    out << "any(index:" << value.type_index() <<", ";
    switch (value.type_index()) {
        case any::index<std::monostate>: out << "[empty]"; break;
        case any::index<class error>: out << value.to<class error>(); break;
        case any::index<std::int8_t>: out << value.to<std::int8_t>(); break;
        case any::index<std::int16_t>: out << value.to<std::int16_t>(); break;
        case any::index<std::int32_t>: out << value.to<std::int32_t>(); break;
        case any::index<std::int64_t>: out << value.to<std::int64_t>(); break;
        case any::index<float>: out << value.to<float>(); break;
        case any::index<double>: out << value.to<double>(); break;
        case any::index<accessor::text>: out << value.to<accessor::text>(); break;
        case any::index<accessor::binary>: out << value.to<accessor::binary>(); break;
        case any::index<takatori::decimal::triple>: out << value.to<takatori::decimal::triple>(); break;
        case any::index<takatori::datetime::date>: out << value.to<takatori::datetime::date>(); break;
        case any::index<takatori::datetime::time_of_day>: out << value.to<takatori::datetime::time_of_day>(); break;
        case any::index<takatori::datetime::time_point>: out << value.to<takatori::datetime::time_point>(); break;
        case any::index<lob::blob_reference>: out << value.to<lob::blob_reference>(); break;
        case any::index<lob::clob_reference>: out << value.to<lob::clob_reference>(); break;
        case any::index<std::size_t>:  out << value.to<std::size_t>(); break;
    }
    out << ")";
    return out;
}

/**
 * @brief output type name of the give value
 * @return the type name
 */
inline std::string_view type_name(any const& value) {
    switch (value.type_index()) {
        case any::index<std::monostate>: return "empty";
        case any::index<class error>: return "error";
        case any::index<std::int8_t>: return "int1";
        case any::index<std::int16_t>: return "int2";
        case any::index<std::int32_t>: return "int4";
        case any::index<std::int64_t>: return "int8";
        case any::index<float>: return "float4";
        case any::index<double>: return "float8";
        case any::index<accessor::text>: return "character";
        case any::index<accessor::binary>: return "octet";
        case any::index<takatori::decimal::triple>: return "decimal";
        case any::index<takatori::datetime::date>: return "date";
        case any::index<takatori::datetime::time_of_day>: return "time_of_day";
        case any::index<takatori::datetime::time_point>: return "time_point";
        case any::index<lob::blob_reference>: return "blob";
        case any::index<lob::clob_reference>: return "clob";
        case any::index<std::size_t>: return "columm_position";
    }
    std::abort();
}

}  // namespace jogasaki::data
