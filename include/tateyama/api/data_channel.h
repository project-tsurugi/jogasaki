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

namespace tateyama::api {

/**
 * @brief data_channel interface
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
     * @brief acquire new buffer
     * @param size the size in bytes requested for the buffer. On successful acquisition, the acquired buffer
     * is ensured to have at minimum the capacity equal to `size`.
     * @note if the data_channel is ordered, internally the acquired buffer is numbered in the order that this
     * function is called. The caller needs to use the data in buffers after sorting the buffers by the internal index.
     * This is typically the case used for the application output whose ordering is relevant
     * (e.g. SELECT statement with ORDER BY clause)
     * @return the acquired buffer
     */
    virtual buffer& acquire(std::size_t size) = 0;

    /**
     * @brief mark the buffer staged and return it to channel
     * @param buf the buffer to stage
     * @return true when successful
     * @return false otherwise
     */
    virtual bool stage(buffer& buf) = 0;
};

}
