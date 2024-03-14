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
#include "decimal_context.h"

namespace jogasaki::executor::process::impl::expression::details {

decimal::Context standard_decimal_context() {
    // we support (via takatori::decimal::triple) up to 38 precision for sql decimal type, but decimal128 has precision
    // up to 34. So we choose next smallest parameter (arg for IEEEContext needs to be multiples of 32)
    // for enough precision.
    auto c = decimal::IEEEContext(160);
    // in order to avoid out of range that triple can represent, we set the precision to 38
    c.prec(38);
    return c;
}

void ensure_decimal_context() {
    thread_local bool initialized = false;
    if(initialized) return;
    decimal::context = standard_decimal_context();
    initialized = true;
}

std::uint32_t reset_decimal_status() {
    auto ret = decimal::context.status();
    decimal::context.clear_status();
    return ret;
}

}  // namespace jogasaki::executor::process::impl::expression::details
