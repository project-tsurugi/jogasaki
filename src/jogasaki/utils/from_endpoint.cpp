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
#include "from_endpoint.h"

#include <cstddef>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <vector>

#include <takatori/util/exception.h>

namespace jogasaki::utils {

namespace relation = takatori::relation;

using takatori::util::throw_exception;

kvs::end_point_kind from(relation::endpoint_kind type) {
    using t = relation::endpoint_kind;
    using k = kvs::end_point_kind;
    switch(type) {
        case t::unbound: return k::unbound;
        case t::inclusive: return k::inclusive;
        case t::exclusive: return k::exclusive;
        case t::prefixed_inclusive: return k::prefixed_inclusive;
        case t::prefixed_exclusive: return k::prefixed_exclusive;
    }
    throw_exception(std::logic_error{""});
}

}  // namespace jogasaki::utils
