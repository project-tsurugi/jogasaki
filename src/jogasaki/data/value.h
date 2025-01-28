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

#include <cstddef>
#include <cstdint>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>

#include <jogasaki/data/any.h>
#include <jogasaki/data/binary_string_value.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_traits.h>
#include <jogasaki/utils/fail.h>
#include <jogasaki/utils/variant.h>

namespace jogasaki::data {

using jogasaki::data::any;

/**
 * @brief value store with ownership
 * @details value store similar to any class, but this own the value with heap storage (e.g. std::string)
 * and not always trivially copyable.
 */
class value {
public:
    using base_type = std::variant<
        std::monostate,
        std::int8_t,
        std::int16_t,
        std::int32_t,
        std::int64_t,
        float,
        double,
        std::string,  // character
        binary_string_value,  // octet
        runtime_t<meta::field_type_kind::decimal>,
        runtime_t<meta::field_type_kind::date>,
        runtime_t<meta::field_type_kind::time_of_day>,
        runtime_t<meta::field_type_kind::time_point>,
        std::size_t  // for reference column position
    >;

    /**
     * @brief construct empty instance
     */
    value() = default;

    /**
     * @brief construct new instance
     */
    template<typename T, typename E = T>
    value(std::in_place_type_t<T>, E arg) : body_(std::in_place_type<T>, arg) {}

    /**
     * @brief accessor of the content value in given type
     * @warning for bool, the reference to std::int8_t is returned as proxy.
     */
    template<typename T>
    [[nodiscard]]
    std::conditional_t<std::is_same_v<T, bool>, std::int8_t, T>
    const& ref() const {
        using A = std::conditional_t<std::is_same_v<T, bool>, std::int8_t, T>;
        if(auto* p = std::get_if<A>(&body_); p != nullptr) {
            return *p;
        }
        fail_with_exception();
    }

    /**
     * @brief return whether value is assigned to this object
     */
    [[nodiscard]] explicit operator bool() const noexcept;

    /**
     * @brief return whether the value has the content or not
     */
    [[nodiscard]] bool empty() const noexcept;

    /**
     * @brief return the type index
     */
    [[nodiscard]] std::size_t type_index() const noexcept;

    /**
     * @brief return the any class as the view of this object
     */
    [[nodiscard]] any view() const;

    // variant index in value - treat bool as std::int8_t
    template <class T>
    static constexpr std::size_t index =
        alternative_index<std::conditional_t<std::is_same_v<T, bool>, std::int8_t, T>, value::base_type>();

    [[nodiscard]] base_type const& body() const noexcept {
        return body_;
    }
private:
    base_type body_{};
};

// bool is the syntax sugar for std::int8_t
template<>
inline value::value(std::in_place_type_t<bool>, bool arg) : body_(std::in_place_type<std::int8_t>, arg ? 1 : 0) {}

template<>
inline value::value(std::in_place_type_t<bool>, std::int8_t arg) : body_(std::in_place_type<std::int8_t>, arg != 0 ? 1 : 0) {}

}
