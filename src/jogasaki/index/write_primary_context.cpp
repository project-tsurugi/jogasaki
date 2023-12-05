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
#include "write_primary_context.h"

#include <memory>
#include <vector>

#include <jogasaki/data/small_record_store.h>
#include <jogasaki/executor/process/step.h>

namespace jogasaki::index {

write_primary_context::write_primary_context(
    std::unique_ptr<kvs::storage> stg,
    maybe_shared_ptr<meta::record_meta> key_meta,
    maybe_shared_ptr<meta::record_meta> value_meta,
    request_context* rctx
) :
    stg_(std::move(stg)),
    extracted_key_store_(std::move(key_meta)),
    extracted_value_store_(std::move(value_meta)),
    rctx_(rctx)
{}

std::string_view write_primary_context::encoded_key() const noexcept {
    return {static_cast<char*>(key_buf_.data()), key_len_};
}

accessor::record_ref write_primary_context::extracted_key() const noexcept {
    return extracted_key_store_.ref();
}

accessor::record_ref write_primary_context::extracted_value() const noexcept {
    return extracted_value_store_.ref();
}

request_context* write_primary_context::req_context() const noexcept {
    return rctx_;
}

}  // namespace jogasaki::index
