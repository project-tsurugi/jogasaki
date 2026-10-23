/*
 * Copyright 2018-2026 Project Tsurugi.
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
#include <jogasaki/status.h>
#include <jogasaki/relay/blob_session_container.h>
namespace jogasaki {
class request_context;
namespace accessor { class record_ref; }
namespace memory { class lifo_paged_memory_resource; }
namespace executor::conv { class record_assignment; }
namespace executor::expr { class evaluator; }
namespace executor::process::impl { class variables_view; }
}
namespace jogasaki::executor::wrt {
/** Evaluate once, report evaluation errors, then apply the prepared assignment. */
status fill_evaluated_value(expr::evaluator const& eval, conv::record_assignment const& assignment,
    request_context& context, relay::blob_session_container& blob_session,
    process::impl::variables_view variables, memory::lifo_paged_memory_resource& resource,
    accessor::record_ref target);
}
