/*
 * Copyright 2018-2026 Project Tsurugi.
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
#pragma once

#include <jogasaki/utils/cancel_request.h>
#include <jogasaki/utils/line_number_string.h>

// NOLINTNEXTLINE
#define cancel_if_needed(ctx) \
    jogasaki::executor::process::impl::ops::cancel_if_needed_impl( \
        (ctx), __FILE__, line_number_string)

namespace jogasaki::executor::process::impl::ops {

/**
 * @brief check if request cancel is needed, and if so, record the cancel state and abort the context.
 * @details callers should pre-fetch the cancel_enabled flag via utils::request_cancel_enabled() and
 *          guard the call with it: `if (cancel_enabled && cancel_if_needed(ctx))` to avoid repeated
 *          config lookups inside tight loops.
 *          ctx.abort() is called internally when cancel is detected.
 *          The caller is responsible for any additional cleanup (e.g. checkpoint reset, finish()).
 * @tparam Context an operator context type that provides req_context() and abort() members
 * @param ctx the operator context
 * @param filepath source file path passed from the call-site macro (__FILE__)
 * @param position source line string passed from the call-site macro (line_number_string)
 * @return true if cancel was detected; the caller should return aborted immediately
 * @return false otherwise
 */
template<typename Context>
[[nodiscard]] bool cancel_if_needed_impl(
    Context& ctx,
    std::string_view filepath,
    std::string_view position
) {
    if (ctx.req_context() == nullptr) {
        return false;
    }
    auto& res_src = ctx.req_context()->req_info().response_source();
    if (! res_src || ! res_src->check_cancel()) {
        return false;
    }
    utils::cancel_request_impl(*ctx.req_context(), filepath, position);
    ctx.abort();
    return true;
}

}  // namespace jogasaki::executor::process::impl::ops
