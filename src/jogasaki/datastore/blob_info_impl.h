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
#pragma once

#include <tateyama/api/server/blob_info.h>

#include <jogasaki/kvs/database.h>

namespace jogasaki::datastore {

/**
 * @brief blob info implementation
 */
class blob_info_impl : public tateyama::api::server::blob_info {

public:
    /**
     * @brief create empty object
     */
    blob_info_impl() noexcept = default;

    /**
     * @brief destruct the object
     */
    ~blob_info_impl() noexcept override = default;

    blob_info_impl(blob_info_impl const& other) = default;
    blob_info_impl& operator=(blob_info_impl const& other) = default;
    blob_info_impl(blob_info_impl&& other) noexcept = default;
    blob_info_impl& operator=(blob_info_impl&& other) noexcept = default;

    /**
     * @brief create object
     */
    blob_info_impl(
        std::string_view channel_name,
        std::filesystem::path path,
        bool is_temporary
    ) :
        channel_name_(channel_name),
        path_(std::move(path)),
        is_temporary_(is_temporary)
    {}

    /**
     * @brief returns the channel name of the BLOB data.
     * @return the channel name
     */
    [[nodiscard]] std::string_view channel_name() const noexcept override {
        return channel_name_;
    }

    /**
     * @brief returns the path of the file that represents the BLOB.
     * @return the path of the file
     */
    [[nodiscard]] std::filesystem::path path() const noexcept override {
        return path_;
    }

    /**
     * @brief returns whether the file is temporary, and created in the database process.
     * @return true if the BLOB data is temporary
     * @return false otherwise
     */
    [[nodiscard]] bool is_temporary() const noexcept override {
        return is_temporary_;
    }

    /**
     * @brief disposes temporary resources underlying in this BLOB data.
     */
    void dispose() override {
        // no-op for now
    }

private:
    std::string channel_name_{};
    std::filesystem::path path_{};
    bool is_temporary_{};
};

}  // namespace jogasaki::datastore
