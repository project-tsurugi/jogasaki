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

    template<class Visitor>
    decltype(auto) visit(Visitor&& visitor) const {
        return std::visit(std::forward<Visitor>(visitor), body_);
    }
    friend bool operator==(any const&, any const&) noexcept;
private:
    base_type body_{};
};

static_assert(std::is_trivially_copyable_v<any>);
static_assert(std::is_trivially_destructible_v<any>);
static_assert(std::alignment_of_v<any> == 8);
static_assert(sizeof(any) == 56); // this is not a fixed limit, but just to check the size is not unexpectedly large

// bool is the syntax sugar for std::int8_t
template<>
inline any::any(std::in_place_type_t<bool>, bool arg) : body_(std::in_place_type<std::int8_t>, arg ? 1 : 0) {}

template<>
inline any::any(std::in_place_type_t<bool>, std::int8_t arg) : body_(std::in_place_type<std::int8_t>, arg != 0 ? 1 : 0) {}

bool operator==(any const& a, any const& b) noexcept;
inline bool operator!=(any const& a, any const& b) noexcept { return !(a == b); }
std::ostream& operator<<(std::ostream&, any const&);
std::string_view type_name(any const&);

} // namespace jogasaki::data
