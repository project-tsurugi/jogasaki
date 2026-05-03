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

#include <cstddef>
#include <memory>
#include <string>
#include <string_view>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/executor/file/file_writer.h>
#include <jogasaki/executor/file/time_unit_kind.h>
#include <jogasaki/meta/external_record_meta.h>

namespace jogasaki::executor::file {

using takatori::util::maybe_shared_ptr;

class parquet_writer_option {
public:
    parquet_writer_option() = default;

    [[nodiscard]] time_unit_kind time_unit() const noexcept {
        return time_unit_;
    }

    parquet_writer_option& time_unit(time_unit_kind arg) noexcept {
        time_unit_ = arg;
        return *this;
    }

private:
    time_unit_kind time_unit_{time_unit_kind::unspecified};
};

/**
 * @brief parquet file writer
 */
class parquet_writer : public file_writer {
public:
    /**
     * @brief create empty object
     */
    parquet_writer();

    parquet_writer(parquet_writer const& other) = delete;
    parquet_writer& operator=(parquet_writer const& other) = delete;
    parquet_writer(parquet_writer&& other) noexcept;
    parquet_writer& operator=(parquet_writer&& other) noexcept;

    /**
     * @brief create new object
     * @param meta record meta with column names
     * @param opt writer options
     * @details this function is intended to be called from open(). Use open() function because it can report error
     * during initialization.
     */
    explicit parquet_writer(maybe_shared_ptr<meta::external_record_meta> meta, parquet_writer_option opt);

    /**
     * @brief destruct object
     * @details destruct the object closing the file if any opened
     */
    ~parquet_writer() noexcept override;

    /**
     * @brief write the record
     * @param ref the record reference written by the writer
     * @return true when successful
     * @return false otherwise
     */
    bool write(accessor::record_ref ref) override;

    /**
     * @brief close the writer and finish the output file
     * @return true when successful
     * @return false otherwise
     */
    bool close() override;

    /**
     * @brief accessor to the written file path
     */
    [[nodiscard]] std::string path() const noexcept override;

    /**
     * @brief accessor to the number of successful write
     */
    [[nodiscard]] std::size_t write_count() const noexcept override;

    /**
     * @brief close current row group and move to new one
     */
    void new_row_group() override;

    /**
     * @brief factory function to construct the new parquet_writer object
     * @param meta metadata of the written records
     * @param opt writer options
     * @return newly created object on success
     * @return nullptr otherwise
     */
    static std::shared_ptr<parquet_writer>
    open(maybe_shared_ptr<meta::external_record_meta> meta, std::string_view path, parquet_writer_option opt);

    /**
     * @brief accessor to the (approx.) maximum size stored in a row group
     * @details 0 is returned if max is not set
     */
    [[nodiscard]] std::size_t row_group_max_records() const noexcept override;

private:
    class impl;
    std::unique_ptr<impl> impl_;
};

}  // namespace jogasaki::executor::file
