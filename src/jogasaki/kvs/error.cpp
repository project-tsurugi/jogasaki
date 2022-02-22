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
#include "error.h"

#include <glog/logging.h>
#include <sharksfin/StatusCode.h>

#include <jogasaki/logging.h>
#include <jogasaki/status.h>

namespace jogasaki::kvs {

status resolve(sharksfin::StatusCode code) noexcept {
    if(code != sharksfin::StatusCode::OK) {
        DVLOG(log_debug) << "error: " << code;
    }
    switch(code) {
        case sharksfin::StatusCode::OK: return status::ok;
        case sharksfin::StatusCode::NOT_FOUND: return status::not_found;
        case sharksfin::StatusCode::ALREADY_EXISTS: return status::already_exists;
        case sharksfin::StatusCode::USER_ROLLBACK: return status::user_rollback;
        case sharksfin::StatusCode::ERR_UNKNOWN: return status::err_unknown;
        case sharksfin::StatusCode::ERR_IO_ERROR: return status::err_io_error;
        case sharksfin::StatusCode::ERR_INVALID_ARGUMENT: return status::err_invalid_argument;
        case sharksfin::StatusCode::ERR_INVALID_STATE: return status::err_invalid_state;
        case sharksfin::StatusCode::ERR_UNSUPPORTED: return status::err_unsupported;
        case sharksfin::StatusCode::ERR_USER_ERROR: return status::err_user_error;
        case sharksfin::StatusCode::ERR_ABORTED: return status::err_aborted;
        case sharksfin::StatusCode::ERR_ABORTED_RETRYABLE: return status::err_aborted_retryable;
        case sharksfin::StatusCode::ERR_TIME_OUT: return status::err_time_out;
        case sharksfin::StatusCode::ERR_NOT_IMPLEMENTED: return status::err_not_implemented;
        case sharksfin::StatusCode::ERR_ILLEGAL_OPERATION: return status::err_illegal_operation;
    }
    std::abort();
}
}

