/*
 * Copyright 2018-2021 tsurugi project.
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

#include "buffer.h"

#include <tateyama/status.h>

namespace tateyama::api::endpoint {

/**
 * @brief data_channel interface
 * @details this object manages a series of buffers. The caller can acquire and return buffers from this object.
 * The channel is either ordered or unordered. The ordered channel keep track of the order of buffers based of the
 * order they are acquired, while unordered one doesn't care.
 */
class data_channel {
public:
    /**
     * @brief create empty object
     */
    data_channel() = default;

    /**
     * @brief destruct the object
     */
    virtual ~data_channel() = default;

    data_channel(data_channel const& other) = default;
    data_channel& operator=(data_channel const& other) = default;
    data_channel(data_channel&& other) noexcept = default;
    data_channel& operator=(data_channel&& other) noexcept = default;

    /**
     * @brief acquire a new buffer
     * @param size the minimum size in bytes requested for the buffer. When successful, the acquired buffer
     * is ensured to have the capacity equal to, or larger than `size`.
     * @param buf [out] the pointer to the acquired buffer
     * @details the caller can use the acquired buffer freely to write the data. Once it finishes using the buffer,
     * the buffer must be returned by calling the `stage` function.
     * @note if the data_channel is ordered, internally the acquired buffer is numbered in the order that this
     * function is called. The caller needs to use the data in buffers after sorting the buffers by the internal index.
     * This is typically the case used for the application output whose ordering is relevant
     * (e.g. SELECT statement with ORDER BY clause)
     * @note this function is thread-safe and multiple threads can invoke simultaneously.
     * @return status::ok when successful
     * @return other status code when error occurs
     */
    virtual status acquire(std::size_t size, buffer*& buf) = 0;

    /**
     * @brief mark the buffer staged and return it to channel
     * @param buf the buffer to stage
     * @details by staging the buffer, caller declares finishing writing to the buffer and passes it to components
     * that subsequently read data from the buffer. The caller must not call any of the buffer function any more.
     * @return status::ok when successful
     * @return other status code when error occurs
     */
    virtual status stage(buffer& buf) = 0;

    /**
     * @brief discard the buffer
     * @param buf the buffer to discard
     * @details by discarding the buffer, caller declares to stop writing to the buffer and return it to channel.
     * The content of the buffer will not be read and the caller must not call any of the buffer function any more.
     * @return status::ok when successful
     * @return other status code when error occurs
     */
    virtual status discard(buffer& buf) = 0;
};

}
