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
#include "offer_context.h"

#include <jogasaki/data/small_record_store.h>
#include <jogasaki/executor/io/record_writer.h>
#include "context_base.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::maybe_shared_ptr;

offer_context::offer_context(
    abstract::task_context* ctx,
    maybe_shared_ptr<meta::record_meta> meta,
    variable_table& variables,
    context_base::memory_resource* resource,
    context_base::memory_resource* varlen_resource
) :
    context_base(ctx, variables, resource, varlen_resource),
    store_(std::move(meta))
{}

operator_kind offer_context::kind() const noexcept {
    return operator_kind::offer;
}

data::small_record_store& offer_context::store() noexcept {
    return store_;
}

void offer_context::release() {
    if(writer_) {
        writer_->flush();
        writer_->release();
        writer_ = nullptr;
    }
}

}


