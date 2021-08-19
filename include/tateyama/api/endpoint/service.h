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

namespace tateyama::api::endpoint {

/**
 * @brief tateyama service interface
 * @details this object provides access to send request and receive response to/from tateyama server application
 */
class service {
public:
    /**
     * @brief create empty object
     */
    service() = default;

    /**
     * @brief destruct the object
     */
    virtual ~service() = default;

    service(service const& other) = default;
    service& operator=(service const& other) = default;
    service(service&& other) noexcept = default;
    service& operator=(service&& other) noexcept = default;

    /**
     * @brief tateyama endpoint service interface
     * @param req the request
     * @param res the response
     * @details this function provides API for tateyama AP service (routing requests to server AP and
     * returning response and application output through data channels.)
     * Endpoint uses this function to transfer request to AP and receive its response and output.
     * `request`, `response` and IF objects (such as data_channel) derived from them are expected to be implemented
     * by the Endpoint so that it provides necessary information in request, and receive result or notification
     * in response.
     * This function is asynchronous, that is, it returns as soon as the request is submitted and scheduled.
     * The caller monitors the response and data_channel to check the progress. See tateyama::api::endpoint::response
     * for details. Once request is transferred and fulfilled by the server AP, the response and data_channel
     * objects member functions are called back to transfer the result.
     * @note this function is thread-safe. Multiple client threads sharing this database object can call simultaneously.
     */
    virtual tateyama::status operator()(
        std::shared_ptr<tateyama::api::endpoint::request const> req,
        std::shared_ptr<tateyama::api::endpoint::response> res
    ) = 0;
};

/**
 * @brief factory method for tateyama application service
 * @param db the underlying database for the service
 * TODO This function is temporarily. Assuming the jogasaki db is only the server application.
 * @return service api object
 * @return nullptr if error occurs on creation
 */
std::unique_ptr<service> create_service(jogasaki::api::database& db);

}
