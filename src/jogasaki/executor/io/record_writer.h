/*
 * Copyright 2018-2020 tsurugi project.
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

#include <jogasaki/accessor/record_ref.h>

namespace jogasaki::executor::io {

/**
 * @brief record writer interface
 * @details processes use this API to write its processing data to pass down to exchanges
 * record_writer manages internal buffer and implicitly flush when it's full.
 * Caller will be notified when flush happens, so that it can considers yielding thread to other processes.
 */
class record_writer {
public:

    /**
     * @brief write the record and flush the current buffer if it's full
     * @return true if successful
     * @return false otherwise
     */
    virtual bool write(accessor::record_ref rec) = 0;

    /**
     * @brief force flush to provide the buffered data to downstream instantly
     * @details current buffer is finalized and will be presented for the downstream.
     * On next write() call, new buffer will be allocated and used to store the incoming data.
     */
    virtual void flush() = 0;

    /**
     * @brief declare ending use of this object and return to owner
     */
    virtual void release() = 0;

    /**
     * @brief creates a new instance.
     */
    record_writer() = default;

    /**
     * @brief destroys this object.
     */
    virtual ~record_writer() = default;

    /**
     * @brief creates a new instance.
     * @param other the source object
     */
    record_writer(record_writer const& other) = default;

    /**
     * @brief assigns the given object.
     * @param other the source object
     * @return this
     */
    record_writer& operator=(record_writer const& other) = default;

    /**
     * @brief creates a new instance.
     * @param other the source object
     */
    record_writer(record_writer&& other) noexcept = default;

    /**
     * @brief assigns the given object.
     * @param other the source object
     * @return this
     */
    record_writer& operator=(record_writer&& other) noexcept = default;
};

/// @brief equality comparison operator
inline bool operator==(record_writer const& a, record_writer const& b) noexcept {
    return std::addressof(a) == std::addressof(b);
}

/// @brief inequality comparison operator
inline bool operator!=(record_writer const& a, record_writer const& b) noexcept {
    return !(a == b);
}

}

