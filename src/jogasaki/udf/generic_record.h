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
#include <condition_variable>
#include <queue>
#include <chrono>
#include <mutex>

#include "error_info.h"
namespace plugin::udf {

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
    [[nodiscard]] virtual bool has_next() = 0;
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
     * @details if error occurs during retrieval, the resulting record will contain its error information.
     * @param record the record to store the retrieved data, including its error information.
     *               The contents will be modified if and only if the return value is status_type::ok or status_type::error.
     * @return status_type::ok if a record was successfully retrieved
     * @return status_type::error if an erroneous record was retrieved
     * @return status_type::end_of_stream if the end of the stream has been reached
     * @return status_type::not_ready if the next record is not yet available
     */
    [[nodiscard]] virtual status_type try_next(generic_record& record) = 0;

    /**
     * @brief retrieves the next record from the stream, waiting up to the specified timeout.
     * @details if error occurs during retrieval, the resulting record will contain its error information.
     * @param record the record to store the retrieved data, including its error information.
     *               The contents will be modified if and only if the return value is status_type::ok or status_type::error.
     * @param timeout the maximum duration to wait for the next record, or `std::nullopt` to wait indefinitely
     * @return status_type::ok if a record was successfully retrieved
     * @return status_type::error if an erroneous record was retrieved
     * @return status_type::end_of_stream if the end of the stream has been reached
     * @return status_type::not_ready if the operation timed out before a record could be retrieved
     */
    [[nodiscard]] virtual status_type
    next(generic_record& record, std::optional<std::chrono::milliseconds> timeout = std::nullopt) = 0;

    /**
     * @brief closes the stream and releases associated resources.
     */
    virtual void close() = 0;
};
}  // namespace plugin::udf
