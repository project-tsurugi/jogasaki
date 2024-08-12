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
#include "forward_info.h"

#include <memory>
#include <set>
#include <type_traits>
#include <utility>
#include <vector>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/meta/record_meta.h>

namespace jogasaki::executor::exchange::forward {

using takatori::util::maybe_shared_ptr;

forward_info::forward_info(
    maybe_shared_ptr<meta::record_meta> meta,
    std::optional<std::size_t> limit
) :
    meta_(std::move(meta)),
    limit_(limit)
{}

maybe_shared_ptr<meta::record_meta> const& forward_info::record_meta() const noexcept {
    return meta_;
}

std::optional<std::size_t> forward_info::limit() const noexcept {
    return limit_;
}

}  // namespace jogasaki::executor::exchange::forward
