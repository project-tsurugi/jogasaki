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

#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#include "error_info.h"
namespace plugin::udf {

// @see https://protobuf.dev/programming-guides/proto3/
class generic_record_cursor {
public:

    generic_record_cursor() = default;
    generic_record_cursor(const generic_record_cursor&) = delete;
    generic_record_cursor& operator=(const generic_record_cursor&) = delete;
    generic_record_cursor(generic_record_cursor&&) = delete;
    generic_record_cursor& operator=(generic_record_cursor&&) = delete;
    virtual ~generic_record_cursor() = default;

    [[nodiscard]] virtual std::optional<bool> fetch_bool() = 0;
    [[nodiscard]] virtual std::optional<std::int32_t> fetch_int4() = 0;
    [[nodiscard]] virtual std::optional<std::int64_t> fetch_int8() = 0;
    [[nodiscard]] virtual std::optional<std::uint32_t> fetch_uint4() = 0;
    [[nodiscard]] virtual std::optional<std::uint64_t> fetch_uint8() = 0;
    [[nodiscard]] virtual std::optional<float> fetch_float() = 0;
    [[nodiscard]] virtual std::optional<double> fetch_double() = 0;
    [[nodiscard]] virtual std::optional<std::string> fetch_string() = 0;
    [[nodiscard]] virtual bool has_next() = 0;
};

class generic_record {
public:

    generic_record() = default;
    generic_record(const generic_record&) = delete;
    generic_record& operator=(const generic_record&) = delete;
    generic_record(generic_record&&) = delete;
    generic_record& operator=(generic_record&&) = delete;
    virtual ~generic_record() = default;

    virtual void reset() = 0;

    virtual void add_bool(bool value) = 0;
    virtual void add_bool_null() = 0;

    virtual void add_int4(std::int32_t value) = 0;
    virtual void add_int4_null() = 0;

    virtual void add_int8(std::int64_t value) = 0;
    virtual void add_int8_null() = 0;

    virtual void add_uint4(std::uint32_t value) = 0;
    virtual void add_uint4_null() = 0;

    virtual void add_uint8(std::uint64_t value) = 0;
    virtual void add_uint8_null() = 0;

    virtual void add_float(float value) = 0;
    virtual void add_float_null() = 0;

    virtual void add_double(double value) = 0;
    virtual void add_double_null() = 0;

    virtual void add_string(std::string value) = 0;
    virtual void add_string_null() = 0;

    virtual void set_error(const error_info& status) = 0;
    [[nodiscard]] virtual std::optional<error_info>& error() noexcept = 0;
    [[nodiscard]] virtual const std::optional<error_info>& error() const noexcept = 0;

    [[nodiscard]] virtual std::unique_ptr<generic_record_cursor> cursor() const = 0;
};

}  // namespace plugin::udf
