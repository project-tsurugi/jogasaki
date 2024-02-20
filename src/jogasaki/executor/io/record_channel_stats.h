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

#include <cstddef>
#include <atomic>

namespace jogasaki::executor::io {

/**
 * @brief stats information on record data channel
 */
class record_channel_stats {
public:
    /**
     * @brief create empty object
     */
    record_channel_stats() = default;

    /**
     * @brief destruct the object
     */
    ~record_channel_stats() = default;

    record_channel_stats(record_channel_stats const& other) = delete;
    record_channel_stats& operator=(record_channel_stats const& other) = delete;
    record_channel_stats(record_channel_stats&& other) noexcept = delete;
    record_channel_stats& operator=(record_channel_stats&& other) noexcept = delete;

    /**
     * @brief getter for total record count
     * @details the sum of all writers write record count is returned
     */
    [[nodiscard]] std::size_t total_record_count() const noexcept {
        return total_record_count_;
    }

    /**
     * @brief setter for total record count
     */
    void total_record_count(std::size_t arg) noexcept {
        total_record_count_ = arg;
    }

    /**
     * @brief setter for total record count
     */
    void add_total_record(std::size_t arg) noexcept {
        total_record_count_.fetch_add(arg);
    }

private:
    std::atomic_size_t total_record_count_{};
};

}  // namespace jogasaki::executor::io
