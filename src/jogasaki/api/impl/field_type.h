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

#include <takatori/util/fail.h>

#include <jogasaki/api/field_type.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>

namespace jogasaki::api::impl {

using takatori::util::fail;

inline api::field_type_kind from(meta::field_type_kind k) noexcept {
    using kind = api::field_type_kind;
    switch(k) {
        case meta::field_type_kind::undefined: return kind::undefined;
        case meta::field_type_kind::boolean: return kind::boolean;
        case meta::field_type_kind::int1: return kind::int1;
        case meta::field_type_kind::int2: return kind::int2;
        case meta::field_type_kind::int4: return kind::int4;
        case meta::field_type_kind::int8: return kind::int8;
        case meta::field_type_kind::float4: return kind::float4;
        case meta::field_type_kind::float8: return kind::float8;
        case meta::field_type_kind::decimal: return kind::decimal;
        case meta::field_type_kind::character: return kind::character;
        case meta::field_type_kind::bit: return kind::bit;
        case meta::field_type_kind::date: return kind::date;
        case meta::field_type_kind::time_of_day: return kind::time_of_day;
        case meta::field_type_kind::time_point: return kind::time_point;
        case meta::field_type_kind::time_interval: return kind::time_interval;
        case meta::field_type_kind::array: return kind::array;
        case meta::field_type_kind::record: return kind::record;
        case meta::field_type_kind::unknown: return kind::unknown;
        case meta::field_type_kind::row_reference: return kind::row_reference;
        case meta::field_type_kind::row_id: return kind::row_id;
        case meta::field_type_kind::declared: return kind::declared;
        case meta::field_type_kind::extension: return kind::extension;
        case meta::field_type_kind::pointer: return kind::pointer;
    }
    fail();
}

/**
 * @brief type information for a field
 */
class field_type : public api::field_type {
public:

    /**
     * @brief construct empty object (kind undefined)
     */
    constexpr field_type() noexcept = default;

    /**
     * @brief construct empty object (kind undefined)
     */
    field_type(meta::field_type type) noexcept : type_(std::move(type)) {}

    /**
     * @brief getter for type kind
     */
    [[nodiscard]] api::field_type_kind kind() const noexcept override {
        return from(type_.kind());
    };

private:
    meta::field_type type_{};
};

} // namespace

