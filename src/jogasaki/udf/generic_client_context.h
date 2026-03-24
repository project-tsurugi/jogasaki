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

#include <chrono>
#include <optional>
#include <string_view>

#include <grpcpp/client_context.h>

namespace plugin::udf {

class generic_client_context {
  public:
    generic_client_context() = default;
    ~generic_client_context() = default;

    generic_client_context(generic_client_context const&) = delete;
    generic_client_context& operator=(generic_client_context const&) = delete;
    generic_client_context(generic_client_context&&) = delete;
    generic_client_context& operator=(generic_client_context&&) = delete;

    /**
     * @brief returns gRPC client context to communicate to UDF implementation.
     * @return gRPC client context
     */
    [[nodiscard]] grpc::ClientContext& grpc_context() noexcept;
    [[nodiscard]] grpc::ClientContext const& grpc_context() const noexcept;
    /**
     * @brief returns timeout value for gRPC calls.
     * @return timeout value
     * @return empty if no timeout is set
     *
     * @note This reflects only the value stored in this object.
     *       It may not represent the actual deadline set on the underlying
     *       grpc::ClientContext.
     */
    [[nodiscard]] std::optional<std::chrono::milliseconds> timeout() const noexcept;
    /**
     * @brief sets timeout value for gRPC calls.
     *
     * @param value timeout value
     * @param value std::nullopt means no timeout (no deadline will be set)
     *
     * @note This setting is intended to be applied at most once.
     *       Once a deadline is set on the underlying grpc::ClientContext,
     *       it cannot be cleared. Passing std::nullopt does not remove
     *       an already applied deadline.
     */
    void timeout(std::optional<std::chrono::milliseconds> value) noexcept;
    /**
     * @brief checks whether debug logging is enabled.
     * @return true if debug logging is enabled
     * @return false otherwise
     */
    [[nodiscard]] bool is_debug_enabled() const noexcept;
    void debug_enabled(bool value) noexcept;
    /**
     * @brief logs debug message.
     * @param message debug message
     * @see is_debug_enabled()
     */
    void log_debug(std::string_view message) const;

  private:
    void apply_timeout() noexcept;

    grpc::ClientContext grpc_context_{};
    std::optional<std::chrono::milliseconds> timeout_{};
    bool debug_enabled_{false};
};

} // namespace plugin::udf
