/*
 * Copyright 2018-2024 Project Tsurugi.
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
#include "secondary_context.h"

#include <utility>

#include <jogasaki/kvs/storage.h>
#include <jogasaki/request_context.h>

namespace jogasaki::index {

secondary_context::secondary_context(
    std::unique_ptr<kvs::storage> stg,
    request_context* rctx
) :
    stg_(std::move(stg)),
    rctx_(rctx)
{}

request_context* secondary_context::req_context() const noexcept {
    return rctx_;
}

}  // namespace jogasaki::index
