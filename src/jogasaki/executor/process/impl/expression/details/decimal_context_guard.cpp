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
#include "decimal_context_guard.h"

#include <decimal.hh>
#include <exception>
#include <ostream>
#include <glog/logging.h>

namespace jogasaki::executor::process::impl::expression::details {

decimal_context_guard& decimal_context_guard::round(std::int32_t round) {
    round_ = decimal::context.round();
    decimal::context.round(round);
    round_set_ = true;
    return *this;
}

decimal_context_guard::~decimal_context_guard() noexcept {
    try {
        if(round_set_) {
            decimal::context.round(round_);
        }
    } catch (std::exception const& e) {
        LOG(ERROR) << "unexpected error - failed to restore decimal context round mode: " << e.what();
    }
}

}  // namespace jogasaki::executor::process::impl::expression::details
