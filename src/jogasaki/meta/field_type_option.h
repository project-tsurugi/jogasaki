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

#include <jogasaki/accessor/text.h>
#include <jogasaki/meta/field_type_kind.h>

namespace jogasaki::meta {

// placeholders for optional information for types
// TODO implement for production
// add == operators to suppress compile errors
struct array_field_option { explicit array_field_option(std::size_t size) : size_(size) {} std::size_t size_; }; //NOLINT
struct record_field_option {};
struct unknown_field_option {};
struct row_reference_field_option {};
struct row_id_field_option {};
struct declared_field_option {};
struct extension_field_option {};

struct time_of_day_field_option {
    time_of_day_field_option() = default;

    explicit time_of_day_field_option(std::int64_t tz_min_offset) :
        tz_min_offset_(tz_min_offset)
    {}

    std::int64_t tz_min_offset_{};  //NOLINT
};

struct time_point_field_option {
    time_point_field_option() = default;

    explicit time_point_field_option(std::int64_t tz_min_offset) :
        tz_min_offset_(tz_min_offset)
    {}

    std::int64_t tz_min_offset_{};  //NOLINT
}; //NOLINT

// temporary implementation to test field types with options in general FIXME
inline bool operator==(array_field_option const& a, array_field_option const& b) noexcept {
    return a.size_ == b.size_;
}
inline bool operator==(time_of_day_field_option const& a, time_of_day_field_option const& b) noexcept {
    return a.tz_min_offset_ == b.tz_min_offset_;
}
inline bool operator==(time_point_field_option const& a, time_point_field_option const& b) noexcept {
    return a.tz_min_offset_ == b.tz_min_offset_;
}

} // namespace

