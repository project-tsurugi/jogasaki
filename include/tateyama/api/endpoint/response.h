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

#include "data_channel.h"
#include "response_code.h"

namespace tateyama::api::endpoint {

/**
 * @brief response interface
 */
class response {
public:
    /**
     * @brief create empty object
     */
    response() = default;

    /**
     * @brief destruct the object
     */
    virtual ~response() = default;

    response(response const& other) = default;
    response& operator=(response const& other) = default;
    response(response&& other) noexcept = default;
    response& operator=(response&& other) noexcept = default;

    //    virtual void session_id(std::size_t session) = 0;
    //    virtual void requester_id(std::size_t id) = 0;

    /**
     * @brief setter of the tateyama response status
     * @param st the status code of the response
     * @details This is the status code on the tateyama layer. If application error occurs, the details are stored in
     * the body.
     */
    void code(response_code code) {
        response_code_ = code;
    }

    /**
     * @brief setter of the tateyama error message
     * @param msg the error message
     * @details This is the error message on the tateyama layer. If application error occurs, its detailed message is
     * stored in the body.
     */
    void message(std::string_view msg) {
        message_ = msg;
    }

    /**
     * @brief notify completion of the response
     * @detail this function is called to notify the response body is filled and accessible.
     * @return status::ok when successful
     * @return other code when error occurs
     */
    virtual status complete() = 0;

    /**
     * @brief write data to the response body
     * @param data the pointer to the data written to the response body
     * @param sz the size of the data
     * @pre complete() function of this object is not yet called
     * @return status::ok when successful
     * @return other code when error occurs
     */
    virtual status write_body(char const* data, std::size_t sz) = 0;

    /**
     * @brief retrieve output data channel
     * @param name the name of the output
     * @param ch [out] the data channel for the given name
     * @detail this function provides the named data channel for the application output
     * @return status::ok when successful
     * @return other code when error occurs
     */
    virtual status output_channel(std::string_view name, data_channel*& ch) = 0;

    /**
     * @brief mark the data channel staged and return its ownership
     * @param ch the data channel to stage
     * @details staging the channel declares finishing using the channel and transfer the channel together with its
     * buffers to the downstream components that read data from the buffers. This function automatically calls
     * data_channel::stage() for all the buffers that belong to this channel.
     * The caller must not call any of the `ch` member functions any more.
     * @return status::ok when successful
     * @return other status code when error occurs
     */
    virtual status stage(data_channel& ch) = 0;

    /**
     * @brief discard the buffer
     * @param ch the data channel to discard
     * @details by discarding the data channel, caller declares to stop writing to the buffer of the data channel and
     * return the ownership of the channel and buffers. This function automatically calls
     * data_channel::discard() for all the buffers that belong to this channel.
     * The caller must not call any of the `ch` member functions any more.
     * @return status::ok when successful
     * @return other status code when error occurs
     */
    virtual status discard (data_channel& ch) = 0;

protected:
    [[nodiscard]] response_code code() const noexcept {
        return response_code_;
    }

    [[nodiscard]] std::string_view message() const noexcept {
        return message_;
    }

private:
    response_code response_code_{};
    std::string message_{};
};

}
