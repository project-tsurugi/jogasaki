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
 * @brief submit a task to process commit response
 * @details this function submits a task to invoke commit response callback and scheduler following teardown task
 * @param rctx the request context
 * @param kind the kind of the commit response
 * @param is_error whether the commit response is an error
 * @param is_canceled whether the commit request is canceled in the middle of processing. If this is true, is_error is ignored.
 * @param teardown_try_on_suspended_worker whether to submit teardown on the suspended worker
 */
void submit_commit_response(
    std::shared_ptr<request_context> rctx,
    commit_response_kind kind,
    bool is_error,
    bool is_canceled,
    bool teardown_try_on_suspended_worker
);

}  // namespace jogasaki
