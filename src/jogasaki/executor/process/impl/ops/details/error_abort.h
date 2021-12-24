/*
 * Copyright 2018-2020 tsurugi project.
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
#include "../operation_status.h"

namespace jogasaki::executor::process::impl::ops::details {

template <class T>
operation_status error_abort(T& ctx, status res) {
    ctx.abort();
    ctx.req_context()->status_code(res);
    return {operation_status_kind::aborted};
}

}



