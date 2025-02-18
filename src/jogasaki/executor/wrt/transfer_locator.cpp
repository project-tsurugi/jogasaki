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
#include "transfer_locator.h"

#include <memory>

namespace jogasaki::executor::wrt {

void transfer_blob_locators(
    request_context& dest,
    expr::evaluator_context& src
) {
    for (auto&& e : src.lob_locators()) {
        dest.add_locator(e);
    }
}

}  // namespace jogasaki::executor::wrt
