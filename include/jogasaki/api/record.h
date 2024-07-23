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

#include <jogasaki/api/field_type_traits.h>

namespace jogasaki::api {

/**
 * @brief record object in the result set
 * @details this interface represents a record in the query result and provides accessor to field values
 */
class record {
public:
    using kind = field_type_kind;

    template<kind Kind>
    using runtime_type = typename field_type_traits<Kind>::runtime_type;

    /**
     * @brief construct empty object
     */
    record() = default;

    /**
     * @brief copy construct
     */
    record(record const&) = default;

    /**
     * @brief move construct
     */
    record(record &&) = default;

    /**
     * @brief copy assign
     */
    record& operator=(record const&) = default;

    /**
     * @brief move assign
     */
    record& operator=(record &&) = default;

    /**
     * @brief destruct record
     */
    virtual ~record() = default;

    /**
     * @brief getter for field values
     * @param index indicate the field offset originated at 0. This must be smaller than the field count.
     * @return the value of given type
     * @attention the returned value is valid only when :
     * - the field is not nullable (i.e. record_meta::nullable() returns false), or
     * - the field value is not null (i.e. is_null() returns false)
     * Otherwise, the returned value is invalid and should be ignored.
     */
    [[nodiscard]] virtual runtime_type<kind::boolean> get_boolean(std::size_t index) const = 0;
    [[nodiscard]] virtual runtime_type<kind::int4> get_int4(std::size_t index) const = 0;
    [[nodiscard]] virtual runtime_type<kind::int8> get_int8(std::size_t index) const = 0;
    [[nodiscard]] virtual runtime_type<kind::float4> get_float4(std::size_t index) const = 0;
    [[nodiscard]] virtual runtime_type<kind::float8> get_float8(std::size_t index) const = 0;
    [[nodiscard]] virtual runtime_type<kind::decimal> get_decimal(std::size_t index) const = 0;
    [[nodiscard]] virtual runtime_type<kind::character> get_character(std::size_t index) const = 0;
    [[nodiscard]] virtual runtime_type<kind::octet> get_octet(std::size_t index) const = 0;
    [[nodiscard]] virtual runtime_type<kind::date> get_date(std::size_t index) const = 0;
    [[nodiscard]] virtual runtime_type<kind::time_of_day> get_time_of_day(std::size_t index) const = 0;
    [[nodiscard]] virtual runtime_type<kind::time_point> get_time_point(std::size_t index) const = 0;

    /**
     * @brief getter for nullilty
     * @param index indicate the field offset originated at 0. This must be smaller than the field count.
     * @return boolean flag indicating whether the field value is null or not
     */
    [[nodiscard]] virtual bool is_null(size_t index) const noexcept = 0;

    /**
     * @brief appends string representation of the record
     * @param out the target output
     * @param value the target value
     * @return the output stream
     */
    friend inline std::ostream& operator<<(std::ostream& out, record const& value) {
        value.write_to(out);
        return out;
    }

protected:
    virtual void write_to(std::ostream& os) const noexcept = 0;
};

}

