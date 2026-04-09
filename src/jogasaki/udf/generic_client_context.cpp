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
#include <optional>

#include <glog/logging.h>
#include <jogasaki/logging.h>
#include <jogasaki/udf/log/logging_prefix.h>

#include "generic_client_context.h"

namespace plugin::udf {

grpc::ClientContext& generic_client_context::grpc_context() noexcept { return grpc_context_; }

grpc::ClientContext const& generic_client_context::grpc_context() const noexcept {
    return grpc_context_;
}

std::optional<std::chrono::milliseconds> generic_client_context::timeout() const noexcept {
    return timeout_;
}

void generic_client_context::timeout(std::optional<std::chrono::milliseconds> value) noexcept {
    timeout_ = value;
    apply_timeout();
}

bool generic_client_context::is_debug_enabled() const noexcept { return debug_enabled_; }

void generic_client_context::debug_enabled(bool value) noexcept { debug_enabled_ = value; }

void generic_client_context::log_debug(std::string_view message) const {
    if (!debug_enabled_) { return; }
    VLOG(jogasaki::log_debug) << jogasaki::udf::log::prefix << message;
}

void generic_client_context::apply_timeout() noexcept {
    if (!timeout_) { return; }

    grpc_context_.set_deadline(std::chrono::system_clock::now() + *timeout_);
}

} // namespace plugin::udf
