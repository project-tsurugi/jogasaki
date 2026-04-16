/*
 * Copyright 2018-2026 Project Tsurugi.
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
#include <iosfwd>
#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "generic_record.h"

namespace plugin::udf {

using value_type =
    std::variant<std::monostate, bool, std::int32_t, std::int64_t, std::uint32_t, std::uint64_t,
        float, double, std::string, bytes_value, decimal_value, date_value, local_time_value,
        local_datetime_value, offset_datetime_value, blob_reference_value, clob_reference_value>;

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
    void add_bytes(bytes_value value) override;
    void add_bytes_null() override;
    void add_decimal(decimal_value value) override;
    void add_decimal_null() override;
    void add_date(date_value value) override;
    void add_date_null() override;
    void add_local_time(local_time_value value) override;
    void add_local_time_null() override;
    void add_local_datetime(local_datetime_value value) override;
    void add_local_datetime_null() override;
    void add_offset_datetime(offset_datetime_value value) override;
    void add_offset_datetime_null() override;
    void add_blob_reference(blob_reference_value value) override;
    void add_blob_reference_null() override;
    void add_clob_reference(clob_reference_value value) override;
    void add_clob_reference_null() override;
    [[nodiscard]] std::unique_ptr<generic_record_cursor> cursor() const override;
    [[nodiscard]] std::optional<error_info>& error() noexcept override;
    [[nodiscard]] std::optional<error_info> const& error() const noexcept override;
    void set_error(error_info const& status) override;
    void assign_from(generic_record_impl&& other) noexcept;
    [[nodiscard]] std::string debug_string() const;
    void dump(std::ostream& os) const;

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
    [[nodiscard]] std::optional<bytes_value> fetch_bytes() override;
    [[nodiscard]] std::optional<decimal_value> fetch_decimal() override;
    [[nodiscard]] std::optional<date_value> fetch_date() override;
    [[nodiscard]] std::optional<local_time_value> fetch_local_time() override;
    [[nodiscard]] std::optional<local_datetime_value> fetch_local_datetime() override;
    [[nodiscard]] std::optional<offset_datetime_value> fetch_offset_datetime() override;
    [[nodiscard]] std::optional<blob_reference_value> fetch_blob_reference() override;
    [[nodiscard]] std::optional<clob_reference_value> fetch_clob_reference() override;
    [[nodiscard]] bool has_next() override;
    [[nodiscard]] runtime_type_kind current_kind() const override;
    [[nodiscard]] bool current_is_null() const override;

  private:
    std::vector<value_type> const& values_;
    std::size_t index_ = 0;
};
template <class> struct always_false : std::false_type {};

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
    status_type next(
        generic_record& record, std::optional<std::chrono::milliseconds> timeout) override;
    [[nodiscard]] std::string debug_string() const;
    friend std::ostream& operator<<(std::ostream& os, generic_record_impl const& record);

  private:
    status_type extract_record_from_queue_unlocked(generic_record& record);

    std::queue<std::unique_ptr<generic_record_impl>> queue_;
    bool closed_{false};
    bool eos_{false};

    std::mutex mutex_;
    std::condition_variable cv_;
};

std::ostream& operator<<(std::ostream& os, generic_record_impl const& record);

} // namespace plugin::udf
