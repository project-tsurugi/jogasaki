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
#include "flow.h"

#include <boost/assert.hpp>

#include <jogasaki/executor/exchange/shuffle/run_info.h>

namespace jogasaki::executor::exchange::shuffle {

run_info flow::info() const noexcept {
    BOOST_ASSERT(transfer_completed_);  //NOLINT
    return info_;
}

run_info& flow::updatable_info() noexcept {
    BOOST_ASSERT(! transfer_completed_);  //NOLINT
    return info_;
}

void flow::transfer_completed() noexcept {
    transfer_completed_ = true;
}

} // namespace


