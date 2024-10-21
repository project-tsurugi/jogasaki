/*
 * Copyright 2018-2023 Project Tsurugi.
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

#include <atomic>
#include <memory>
#include <string>
#include <string_view>

#include <jogasaki/error/error_info.h>
#include <jogasaki/commit_response.h>

namespace jogasaki {

/**
 * @brief the callback type used for async commit successful response
 */
using commit_response_callback = std::function<void(commit_response_kind)>;

/**
 * @brief the callback type used for async commit error response
 */
using commit_error_callback = std::function<void(commit_response_kind, status, std::shared_ptr<error::error_info>)>;

/**
 * @brief context object for tx commit processing
 */
class commit_context {
public:
    /**
     * @brief create default context object
     */
    commit_context() = default;

    /**
     * @brief create new context object
     * @param on_response the callback function to be called when the response is ready
     * @param response_kinds the kinds of response to be notified
     */
    commit_context(
        commit_response_callback on_response,
        commit_response_kind_set response_kinds,
        commit_error_callback on_error
    ) : on_response_(std::move(on_response)),
        response_kinds_(response_kinds),
        on_error_(std::move(on_error))
    {}

    commit_response_callback& on_response() noexcept {
        return on_response_;
    }

    commit_response_kind_set& response_kinds() noexcept {
        return response_kinds_;
    }

    commit_error_callback& on_error() noexcept {
        return on_error_;
    }
private:
    commit_response_callback on_response_{};
    commit_response_kind_set response_kinds_{};
    commit_error_callback on_error_{};

};

}  // namespace jogasaki
