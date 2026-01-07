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
#include <optional>

#include <jogasaki/data/any_sequence.h>
#include <jogasaki/data/any_sequence_stream_status.h>

namespace jogasaki::data {

/**
 * @brief abstract interface for streaming any_sequence values.
 * @details this class provides an interface for retrieving sequences of any values from a stream.
 *          it is used as the return type of table-valued functions, abstracting away the
 *          underlying implementation (e.g., gRPC streaming, in-memory data).
 */
class any_sequence_stream {
public:
    using status_type = any_sequence_stream_status;

    any_sequence_stream() = default;
    any_sequence_stream(any_sequence_stream const&) = delete;
    any_sequence_stream& operator=(any_sequence_stream const&) = delete;
    any_sequence_stream(any_sequence_stream&&) = delete;
    any_sequence_stream& operator=(any_sequence_stream&&) = delete;
    virtual ~any_sequence_stream() = default;

    /**
     * @brief attempts to retrieve the next sequence from the stream without blocking.
     * @param sequence the sequence to store the retrieved data.
     *                 the contents will be modified if and only if the return value is
     *                 status_type::ok or status_type::error.
     * @return status_type::ok if a sequence was successfully retrieved
     * @return status_type::error if an error occurred during retrieval
     * @return status_type::end_of_stream if the end of the stream has been reached
     * @return status_type::not_ready if the next sequence is not yet available
     */
    [[nodiscard]] virtual status_type try_next(any_sequence& sequence) = 0;

    /**
     * @brief retrieves the next sequence from the stream, waiting up to the specified timeout.
     * @param sequence the sequence to store the retrieved data.
     *                 the contents will be modified if and only if the return value is
     *                 status_type::ok or status_type::error.
     * @param timeout the maximum duration to wait for the next sequence,
     *                or std::nullopt to wait indefinitely
     * @return status_type::ok if a sequence was successfully retrieved
     * @return status_type::error if an error occurred during retrieval
     * @return status_type::end_of_stream if the end of the stream has been reached
     * @return status_type::not_ready if the operation timed out before a sequence could be retrieved
     */
    [[nodiscard]] virtual status_type next(
        any_sequence& sequence,
        std::optional<std::chrono::milliseconds> timeout) = 0;

    /**
     * @brief closes the stream and releases associated resources.
     */
    virtual void close() = 0;
};

}  // namespace jogasaki::data
