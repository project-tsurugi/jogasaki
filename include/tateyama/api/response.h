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

namespace tateyama::api {

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
    void status_code(status st) = 0;

    /**
     * @brief setter of the tateyama error message
     * @param msg the error message
     * @details This is the error message on the tateyama layer. If application error occurs, its detailed message is
     * stored in the body.
     */
    void message(std::string_view msg) = 0;

    /**
     * @brief notify completion of the response
     * @detail this function is called to notify the response body is filled and accessible.
     * @return true when successful
     * @return false otherwise
     */
    virtual bool complete() = 0;

    /**
     * @brief setter of the response body
     * @param body the binary data of the response body
     */
    virtual std::string_view allocate_body(std::size_t sz) = 0;

    /**
     * @brief retrieve output data channel
     * @param name the name of the output
     * @detail this function provides the named data channel for the application output
     * @return the data_channel object when successful
     * @return nullptr when error occurs
     */
    virtual data_channel* output_channel(std::string_view name) = 0;
};

}
