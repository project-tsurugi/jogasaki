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
#include "set_error.h"

namespace jogasaki::error {

void set_error(request_context &rctx, std::shared_ptr<error::error_info> info) {
    rctx.error_info(std::move(info));
}

void set_tx_error(request_context &rctx, std::shared_ptr<error::error_info> info) {
    if(rctx.error_info(info)) {
        if(rctx.transaction()) {
            rctx.transaction()->error_info(std::move(info));
        }
    }
}

}
