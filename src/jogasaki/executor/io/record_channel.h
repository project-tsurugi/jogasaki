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

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/meta/external_record_meta.h>
#include <jogasaki/status.h>
#include <jogasaki/executor/io/record_channel_stats.h>

#include "record_writer.h"

namespace jogasaki::executor::io {

using takatori::util::maybe_shared_ptr;

/**
 * @brief record channel kind
 */
enum class record_channel_kind : std::size_t {
    undefined = 0,
    record_channel_adapter,
    result_store_channel,
    null_record_channel,
    dump_channel,
};

/**
 * @brief returns string representation of the value.
 * @param value the target valuen
 * @return the corresponding string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(record_channel_kind value) noexcept {
    using namespace std::string_view_literals;
    using kind = record_channel_kind;
    switch (value) {
        case kind::undefined: return "undefined"sv;
        case kind::record_channel_adapter: return "record_channel_adapter"sv;
        case kind::result_store_channel: return "result_store_channel"sv;
        case kind::null_record_channel: return "null_record_channel"sv;
        case kind::dump_channel: return "dump_channel"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, record_channel_kind value) {
    return out << to_string_view(value);
}

/**
 * @brief record data channel interface
 * @details like api::data_channel, this object represents a channel for application output. The difference is
 * this channel handles records while data_channel does binary bytes.
 */
class record_channel {
public:
    /**
     * @brief create empty object
     */
    record_channel() = default;

    /**
     * @brief destruct the object
     */
    virtual ~record_channel() = default;

    record_channel(record_channel const& other) = default;
    record_channel& operator=(record_channel const& other) = default;
    record_channel(record_channel&& other) noexcept = default;
    record_channel& operator=(record_channel&& other) noexcept = default;

    /**
     * @brief acquire a new writer
     * @param wrt [out] the pointer to the acquired writer. The returned value is valid only when
     * the call finishes with status code status::ok.
     * @details the caller can use the acquired writer freely to write the data. Once it finishes using the writer,
     * the writer must be returned by calling the `release` function.
     * @note when multiple writers are acquired from a single channel, the order of the data written by those writers
     * are not managed by this object. Currently, ordered application output is assumed to be written by one writer.
     * (e.g. SELECT statement with ORDER BY clause)
     * @note this function is thread-safe and multiple threads can invoke simultaneously.
     * @pre metadata must be set with `meta()` function before new writer is acquired
     * @return status::ok when successful
     * @return other status code when error occurs
     */
    virtual status acquire(std::shared_ptr<record_writer>& wrt) = 0;

    /**
     * @brief metadata setter
     * @param m the metadata with that the writer uses for the output data
     */
    virtual status meta(maybe_shared_ptr<meta::external_record_meta> m) = 0;

    /**
     * @brief accessor for channel stats
     */
    virtual record_channel_stats& statistics() = 0;

    /**
     * @brief accessor for record channel kind
     */
    [[nodiscard]] virtual record_channel_kind kind() const noexcept = 0;
};

}  // namespace jogasaki::executor::io
