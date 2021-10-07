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

#include <cstddef>
#include <jogasaki/status.h>

namespace jogasaki::api {

/**
 * @brief writer interface
 * @details this writer object provides write function for the application output
 */
class writer {
public:
    /**
     * @brief create empty object
     */
    writer() = default;

    /**
     * @brief destruct the object
     */
    virtual ~writer() = default;

    writer(writer const& other) = default;
    writer& operator=(writer const& other) = default;
    writer(writer&& other) noexcept = default;
    writer& operator=(writer&& other) noexcept = default;

    /**
     * @brief write data
     * @details write out the given data to the application output.
     * This data is opaque binary sequence in this API layer. Typically its format are shared by the endpoint users
     * by having common encoders/decoders.
     * This function can block if internal buffer is full, then it waits for downstream consumers to proceed reading
     * and make some room in the buffer.
     * @param data the pointer to the data to be written
     * @param length the byte length of the data to be written
     * @return status::ok when successful
     * @return other status code when error occurs
     */
    virtual status write(char const* data, std::size_t length) = 0;

    /**
     * @brief commit the written data
     * @details mark the boundary of the written data and notify they are ready to be consumed.
     * To facilitate consumer to proceed, the call site of the writer is expected to call this function periodically at
     * an appropriate boundary of data written (e.g. the boundary of record) so that the consumer can pick up the unit
     * and start processing without waiting in-flight data arrival.
     * @return status::ok when successful
     * @return other status code when error occurs
     */
    virtual status commit() = 0;
};

}
