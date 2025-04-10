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
#include "durability_common.h"

#include <memory>
#include <optional>
#include <ostream>
#include <string_view>
#include <type_traits>
#include <utility>
#include <glog/logging.h>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/impl/request_context_factory.h>
#include <jogasaki/commit_common.h>
#include <jogasaki/commit_profile.h>
#include <jogasaki/configuration.h>
#include <jogasaki/durability_manager.h>
#include <jogasaki/logging.h>
#include <jogasaki/model/task.h>
#include <jogasaki/request_context.h>
#include <jogasaki/request_logging.h>
#include <jogasaki/scheduler/flat_task.h>
#include <jogasaki/scheduler/request_detail.h>
#include <jogasaki/scheduler/schedule_option.h>
#include <jogasaki/scheduler/task_factory.h>
#include <jogasaki/scheduler/task_scheduler.h>
#include <jogasaki/transaction_context.h>
#include <jogasaki/utils/set_cancel_status.h>

namespace jogasaki {

void submit_commit_response(
    std::shared_ptr<request_context> rctx,  //NOLINT(performance-unnecessary-value-param)
    commit_response_kind kind,
    bool is_error,
    bool teardown_try_on_suspended_worker
) {
    auto& ts = *rctx->scheduler();
    ts.schedule_task(
        scheduler::create_custom_task(rctx.get(), [rctx, kind, teardown_try_on_suspended_worker, is_error]() {
            log_commit_end(*rctx);
            if(is_error) {
                rctx->commit_ctx()->on_error()(kind, rctx->status_code(), rctx->error_info());
            } else {
                rctx->commit_ctx()->on_response()(kind);
            }
            scheduler::submit_teardown(*rctx, teardown_try_on_suspended_worker);
            return model::task_result::complete;
        }, model::task_transaction_kind::none)
    );
}

}  // namespace jogasaki
