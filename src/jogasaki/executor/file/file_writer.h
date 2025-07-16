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
#pragma once

#include <string>

#include <jogasaki/accessor/record_ref.h>

namespace jogasaki::executor::file {

/**
 * @brief file writer interface
 */
class file_writer {
public:
    /**
     * @brief create new object
     */
    file_writer() = default;

    file_writer(file_writer const& other) = delete;
    file_writer& operator=(file_writer const& other) = delete;
    file_writer(file_writer&& other) noexcept = default;
    file_writer& operator=(file_writer&& other) noexcept = default;

    /**
     * @brief destruct object
     * @details destruct the object closing the file if any opened
     */
    virtual ~file_writer() noexcept = default;

    /**
     * @brief write the record
     * @param ref the record reference written by the writer
     * @return true when successful
     * @return false otherwise
     */
    virtual bool write(accessor::record_ref ref) = 0;

    /**
     * @brief close the writer and finish the output file
     * @return true when successful
     * @return false otherwise
     */
    virtual bool close() = 0;

    /**
     * @brief accessor to the written file path
     */
    [[nodiscard]] virtual std::string path() const noexcept = 0;

    /**
     * @brief accessor to the number of successful write
     */
    [[nodiscard]] virtual std::size_t write_count() const noexcept = 0;

    /**
     * @brief close current row group and move to new one
     */
    virtual void new_row_group() = 0;

    /**
     * @brief accessor to the (approx.) maximum size stored in a row group
     * @details 0 is returned if max is not set
     */
    [[nodiscard]] virtual std::size_t row_group_max_records() const noexcept = 0;
};

}  // namespace jogasaki::executor::file
