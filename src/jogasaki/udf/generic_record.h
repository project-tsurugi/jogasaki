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

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <string>

#include "error_info.h"

namespace plugin::udf {

struct bytes_value {
    std::string value{};
};

struct decimal_value {
    std::string unscaled_value{};
    std::int32_t exponent{};
};

struct date_value {
    std::int32_t days{};
};

struct local_time_value {
    std::int64_t nanos{};
};

struct local_datetime_value {
    std::int64_t offset_seconds{};
    std::uint32_t nano_adjustment{};
};

struct offset_datetime_value {
    std::int64_t offset_seconds{};
    std::uint32_t nano_adjustment{};
    std::int32_t time_zone_offset{};
};

struct blob_reference_value {
    std::uint64_t storage_id{};
    std::uint64_t object_id{};
    std::uint64_t tag{};
    bool provisioned{};
};

struct clob_reference_value {
    std::uint64_t storage_id{};
    std::uint64_t object_id{};
    std::uint64_t tag{};
    bool provisioned{};
};

enum class runtime_type_kind {
    null_value,
    boolean,
    int4,
    int8,
    uint4,
    uint8,
    float4,
    float8,
    string,
    bytes,
    decimal,
    date,
    local_time,
    local_datetime,
    offset_datetime,
    blob_reference,
    clob_reference,
};

// @see https://protobuf.dev/programming-guides/proto3/
class generic_record_cursor {
  public:
    generic_record_cursor() = default;
    generic_record_cursor(generic_record_cursor const&) = delete;
    generic_record_cursor& operator=(generic_record_cursor const&) = delete;
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
    [[nodiscard]] virtual std::optional<bytes_value> fetch_bytes() = 0;
    [[nodiscard]] virtual std::optional<decimal_value> fetch_decimal() = 0;
    [[nodiscard]] virtual std::optional<date_value> fetch_date() = 0;
    [[nodiscard]] virtual std::optional<local_time_value> fetch_local_time() = 0;
    [[nodiscard]] virtual std::optional<local_datetime_value> fetch_local_datetime() = 0;
    [[nodiscard]] virtual std::optional<offset_datetime_value> fetch_offset_datetime() = 0;
    [[nodiscard]] virtual std::optional<blob_reference_value> fetch_blob_reference() = 0;
    [[nodiscard]] virtual std::optional<clob_reference_value> fetch_clob_reference() = 0;
    [[nodiscard]] virtual bool has_next() = 0;
    [[nodiscard]] virtual runtime_type_kind current_kind() const = 0;
    [[nodiscard]] virtual bool current_is_null() const = 0;
};

class generic_record {
  public:
    generic_record() = default;
    generic_record(generic_record const&) = delete;
    generic_record& operator=(generic_record const&) = delete;
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

    virtual void add_bytes(bytes_value value) = 0;
    virtual void add_bytes_null() = 0;

    virtual void add_decimal(decimal_value value) = 0;
    virtual void add_decimal_null() = 0;

    virtual void add_date(date_value value) = 0;
    virtual void add_date_null() = 0;

    virtual void add_local_time(local_time_value value) = 0;
    virtual void add_local_time_null() = 0;

    virtual void add_local_datetime(local_datetime_value value) = 0;
    virtual void add_local_datetime_null() = 0;

    virtual void add_offset_datetime(offset_datetime_value value) = 0;
    virtual void add_offset_datetime_null() = 0;

    virtual void add_blob_reference(blob_reference_value value) = 0;
    virtual void add_blob_reference_null() = 0;

    virtual void add_clob_reference(clob_reference_value value) = 0;
    virtual void add_clob_reference_null() = 0;

    virtual void set_error(error_info const& status) = 0;
    [[nodiscard]] virtual std::optional<error_info>& error() noexcept = 0;
    [[nodiscard]] virtual std::optional<error_info> const& error() const noexcept = 0;

    [[nodiscard]] virtual std::unique_ptr<generic_record_cursor> cursor() const = 0;
};

class generic_record_stream {
  public:
    /// @brief represents the status of record retrieval.
    using status_type = generic_record_stream_status;
    generic_record_stream() = default;
    generic_record_stream(generic_record_stream const&) = delete;
    generic_record_stream& operator=(generic_record_stream const&) = delete;
    generic_record_stream(generic_record_stream&&) = delete;
    generic_record_stream& operator=(generic_record_stream&&) = delete;
    virtual ~generic_record_stream() = default;
    /**
     * @brief attempts to retrieve the next record from the stream.
     * @details if error occurs during retrieval, the resulting record will contain its error
     * information.
     * @param record the record to store the retrieved data, including its error information.
     *               The contents will be modified if and only if the return value is
     * status_type::ok or status_type::error.
     * @return status_type::ok if a record was successfully retrieved
     * @return status_type::error if an erroneous record was retrieved
     * @return status_type::end_of_stream if the end of the stream has been reached
     * @return status_type::not_ready if the next record is not yet available
     */
    [[nodiscard]] virtual status_type try_next(generic_record& record) = 0;
    /**
     * @brief retrieves the next record from the stream, waiting up to the specified timeout.
     * @details if error occurs during retrieval, the resulting record will contain its error
     * information.
     * @param record the record to store the retrieved data, including its error information.
     *               The contents will be modified if and only if the return value is
     * status_type::ok or status_type::error.
     * @param timeout the maximum duration to wait for the next record, or `std::nullopt` to wait
     * indefinitely
     * @return status_type::ok if a record was successfully retrieved
     * @return status_type::error if an erroneous record was retrieved
     * @return status_type::end_of_stream if the end of the stream has been reached
     * @return status_type::not_ready if the operation timed out before a record could be retrieved
     */
    [[nodiscard]] virtual status_type next(
        generic_record& record, std::optional<std::chrono::milliseconds> timeout) = 0;
    /**
     * @brief closes the stream and releases associated resources.
     */
    virtual void close() = 0;
};

} // namespace plugin::udf
