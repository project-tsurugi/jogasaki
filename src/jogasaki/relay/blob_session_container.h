/*
 * Copyright 2018-2025 Project Tsurugi.
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

#include <memory>
#include <optional>
#include <cstdint>

#include <jogasaki/utils/assert.h>

#include <data_relay_grpc/blob_relay/session.h>

namespace jogasaki::relay {

/**
 * @brief RAII container for BLOB session
 * @details This template class manages the lifecycle of a BLOB session and ensures proper disposal.
 * The session is disposed either by explicit dispose() call or automatically in the destructor.
 * The container does not take ownership of the session pointer - it only ensures dispose() is called.
 * The session pointer must be deleted externally at an appropriate time.
 * @tparam Session the session type to manage (must have a dispose() method)
 */
template<typename Session>
class basic_blob_session_container {
public:
    /**
     * @brief create empty object
     */
    basic_blob_session_container() = default;

    /**
     * @brief create new object
     * @param transaction_id the optional transaction ID to use when creating the session
     */
    explicit basic_blob_session_container(std::optional<std::uint64_t> transaction_id) noexcept :
        transaction_id_(transaction_id)
    {}

    /**
     * @brief destruct the object
     * @details automatically disposes the session if not already disposed
     */
    ~basic_blob_session_container() {
        // to ensure dispose is called
        dispose();
    }

    basic_blob_session_container(basic_blob_session_container const& other) = delete;
    basic_blob_session_container& operator=(basic_blob_session_container const& other) = delete;
    basic_blob_session_container(basic_blob_session_container&& other) noexcept = delete;
    basic_blob_session_container& operator=(basic_blob_session_container&& other) noexcept = delete;

    /**
     * @brief dispose the blob session
     * @details disposes the session and releases all resources associated with it.
     * This method is idempotent - calling it multiple times has no effect after the first call.
     * Note: This method does not delete the session pointer - only calls dispose() on it.
     */
    void dispose() noexcept {
        if (session_ != nullptr) {
            session_->dispose();
            session_ = nullptr;
        }
    }

    /**
     * @brief accessor to the blob session
     * @return pointer to the blob session, or nullptr if not set or already disposed
     */
    [[nodiscard]] Session* get() const noexcept {
        return session_;
    }

    /**
     * @brief accessor to the blob session with lazy initialization
     * @details creates a new session if one does not already exist
     * @return pointer to the blob session (never nullptr after successful creation)
     */
    [[nodiscard]] Session* get_or_create();

    /**
     * @brief check if the container has a valid session
     * @return true if the container has a valid session that has not been disposed
     * @return false otherwise
     */
    [[nodiscard]] bool has_session() const noexcept {
        return session_ != nullptr;
    }

    /**
     * @brief check if the container has a valid session
     * @return true if the container has a valid session that has not been disposed
     * @return false otherwise
     */
    [[nodiscard]] explicit operator bool() const noexcept {
        return has_session();
    }

    /**
     * @brief set the blob session
     * @param session the blob session to manage
     * @attention this method is for testing purposes only.
     * This method can only be called when has_session() == false.
     * Calling this method when a session is already set results in undefined behavior.
     */
    void set(Session* session) {
        assert_with_exception(session_ == nullptr, session_);
        session_ = session;
    }

private:
    Session* session_{};
    std::optional<std::uint64_t> transaction_id_{};
};

/**
 * @brief type alias for BLOB session container with data_relay_grpc::blob_relay::blob_session
 */
using blob_session_container = basic_blob_session_container<data_relay_grpc::blob_relay::blob_session>;

}
