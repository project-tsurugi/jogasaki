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
#include "partitioner.h"

#include <cstddef>
#include <utility>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/executor/hash.h>
#include <jogasaki/meta/record_meta.h>

namespace jogasaki::executor {

using takatori::util::maybe_shared_ptr;

partitioner::partitioner(
    std::size_t partitions,
    maybe_shared_ptr<meta::record_meta> meta
) noexcept:
    partitions_(partitions),
    meta_(std::move(meta)),
    hash_(meta_.get())
{}

std::size_t partitioner::operator()(accessor::record_ref key) const noexcept {
    return hash_(key) % partitions_;
}

}
