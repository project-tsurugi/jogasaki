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

#include "writer.h"

#include <jogasaki/status.h>
#include <jogasaki/api/record_meta.h>

namespace jogasaki::api {

/**
 * @brief data channel interface
 * @details this object represents a channel for application output.
 * The data channel provides a series of writers to write application output.
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
     * @brief acquire a new writer
     * @param wrt [out] the pointer to the acquired writer. The returned value is valid only when the call finishes with status code status::ok.
     * @details the caller can use the acquired writer freely to write the data. Once it finishes using the writer,
     * the writer must be returned by calling the `release` function.
     * @note when multiple writers are acquired from a single channel, the order of the data written by those writers
     * are not managed by this object. Currently, ordered application output is assumed to be written by one writer.
     * (e.g. SELECT statement with ORDER BY clause)
     * @note this function is thread-safe and multiple threads can invoke simultaneously.
     * @return status::ok when successful
     * @return other status code when error occurs
     */
    virtual status acquire(std::shared_ptr<writer>& wrt) = 0;

    /**
     * @brief declare to finish using the writer and return it to channel
     * @param wrt the writer to release
     * @details by releasing the writer, the caller declares to finish using the writer and passes it back.
     * The caller must not call any of the writer function any more.
     * @warning if writer::commit() has not been called after some data is written by writer::write(), it's not ensured
     * that the uncommitted data is consumed by or visible to others. So writer::commit() should be appropriately called before releasing the writer.
     * @note this function is thread-safe and multiple threads can invoke simultaneously.
     * @return status::ok when successful
     * @return other status code when error occurs
     */
    virtual status release(writer& wrt) = 0;

};

}
