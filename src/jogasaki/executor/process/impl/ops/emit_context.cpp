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
#include "emit_context.h"

#include <utility>

#include <jogasaki/data/small_record_store.h>

#include "context_base.h"

namespace jogasaki::executor::process::impl::ops {

emit_context::emit_context(
    class abstract::task_context* ctx,
    variable_table& variables,
    maybe_shared_ptr<meta::record_meta> meta,
    emit_context::memory_resource* resource,
    emit_context::memory_resource* varlen_resource
) :
    context_base(ctx, variables, resource, varlen_resource),
    buffer_(std::move(meta))
{}

data::small_record_store& emit_context::store() noexcept {
    return buffer_;
}

operator_kind emit_context::kind() const noexcept {
    return operator_kind::emit;
}

void emit_context::release() {
    if(writer_) {
        writer_->flush();
        writer_->release();
    }
}

}


