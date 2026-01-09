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
#include <variant>
#include <vector>

#include "enum_types.h"
#include "generic_record.h"
namespace plugin::udf {

using value_type = std::
    variant<std::monostate, bool, std::int32_t, std::int64_t, std::uint32_t, std::uint64_t, float, double, std::string>;
class generic_record_impl : public generic_record {
public:

    void reset() override;
    void add_bool(bool value) override;
    void add_bool_null() override;
    void add_int4(std::int32_t value) override;
    void add_int4_null() override;
    void add_int8(std::int64_t value) override;
    void add_int8_null() override;
    void add_uint4(std::uint32_t value) override;
    void add_uint4_null() override;
    void add_uint8(std::uint64_t value) override;
    void add_uint8_null() override;
    void add_float(float value) override;
    void add_float_null() override;
    void add_double(double value) override;
    void add_double_null() override;
    void add_string(std::string value) override;
    void add_string_null() override;
    [[nodiscard]] std::unique_ptr<generic_record_cursor> cursor() const override;
    [[nodiscard]] std::optional<error_info>& error() noexcept override;
    [[nodiscard]] std::optional<error_info> const& error() const noexcept override;
    void set_error(error_info const& status) override;
    void assign_from(generic_record_impl&& other) noexcept;

private:

    std::vector<value_type> values_;
    std::optional<error_info> err_;
};

class generic_record_cursor_impl : public generic_record_cursor {
public:

    explicit generic_record_cursor_impl(std::vector<value_type> const& values);

    [[nodiscard]] std::optional<bool> fetch_bool() override;
    [[nodiscard]] std::optional<std::int32_t> fetch_int4() override;
    [[nodiscard]] std::optional<std::int64_t> fetch_int8() override;
    [[nodiscard]] std::optional<std::uint32_t> fetch_uint4() override;
    [[nodiscard]] std::optional<std::uint64_t> fetch_uint8() override;
    [[nodiscard]] std::optional<float> fetch_float() override;
    [[nodiscard]] std::optional<double> fetch_double() override;
    [[nodiscard]] std::optional<std::string> fetch_string() override;
    [[nodiscard]] bool has_next() override;

private:

    std::vector<value_type> const& values_;
    std::size_t index_ = 0;
};
template<class>
struct always_false : std::false_type {};

class generic_record_stream_impl final : public generic_record_stream {
public:

    generic_record_stream_impl();
    ~generic_record_stream_impl() override;

    // Copying is explicitly disabled because this class manages a mutex and condition_variable
    generic_record_stream_impl(const generic_record_stream_impl&) = delete;
    generic_record_stream_impl& operator=(const generic_record_stream_impl&) = delete;

    // Move constructor and move assignment are allowed
    generic_record_stream_impl(generic_record_stream_impl&& other) noexcept;
    generic_record_stream_impl& operator=(generic_record_stream_impl&& other) noexcept;

    void push(std::unique_ptr<generic_record_impl> record);
    void end_of_stream();
    void close() override;

    status_type try_next(generic_record& record) override;
    status_type next(generic_record& record, std::optional<std::chrono::milliseconds> timeout) override;

private:

    status_type extract_record_from_queue_unlocked(generic_record& record);

    std::queue<std::unique_ptr<generic_record_impl>> queue_;
    bool closed_{false};
    bool eos_{false};

    std::mutex mutex_;
    std::condition_variable cv_;
};

}  // namespace plugin::udf
