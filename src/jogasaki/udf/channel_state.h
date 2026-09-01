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

#include <string_view>

#include <grpcpp/channel.h>

namespace plugin::udf {

/**
 * @brief returns string representation of the connectivity state
 */
[[nodiscard]] constexpr std::string_view to_string_view(grpc_connectivity_state state) noexcept {
    switch (state) {
        case GRPC_CHANNEL_IDLE: return "IDLE";
        case GRPC_CHANNEL_CONNECTING: return "CONNECTING";
        case GRPC_CHANNEL_READY: return "READY";
        case GRPC_CHANNEL_TRANSIENT_FAILURE: return "TRANSIENT_FAILURE";
        case GRPC_CHANNEL_SHUTDOWN: return "SHUTDOWN";
    }
    return "UNKNOWN";
}

} // namespace plugin::udf
