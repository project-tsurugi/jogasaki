/*
 * Copyright 2018-2025 Project Tsurugi.
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

#include <jogasaki/request_context.h>
#include <jogasaki/commit_response.h>

namespace jogasaki {

/**
 * @brief log transaction ended
 * @param tx the transaction context
 * @param aborted true if aborted, false if committed
 * @param req_info the request info to provide session common info.
 */
void log_end_of_tx(
    transaction_context& tx,
    bool aborted,
    request_info const& req_info
);

/**
 * @brief log the end of commit request
 * @param rctx the request context
 */
void log_end_of_commit_request(request_context& rctx);

/**
 * @brief log both end of tx and commit request
 * @param rctx the request context
 */
void log_end_of_tx_and_commit_request(request_context& rctx);

}  // namespace jogasaki
