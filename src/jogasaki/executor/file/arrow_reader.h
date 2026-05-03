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
#include <optional>
#include <string>
#include <string_view>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/executor/file/file_reader.h>
#include <jogasaki/meta/external_record_meta.h>

namespace jogasaki::executor::file {

using takatori::util::maybe_shared_ptr;

/**
 * @brief parquet file reader
 * @details this reader is created with mapping from parquet fields to record_ref fields that represent values for
 * parameters/placeholders. The reader reads the parquet record and fills the fields according to the mapping.
 */
class arrow_reader : public file_reader {
public:
    static constexpr std::size_t index_unspecified = static_cast<std::size_t>(-1);

    /**
     * @brief create new object
     * @details this function is intended to be called from open(). Use open() function because it can report error
     * during initialization.
     */
    arrow_reader();

    /**
     * @brief destruct object
     * @details destruct the object closing the file if any opened
     */
    ~arrow_reader() noexcept override;

    arrow_reader(arrow_reader const& other) = delete;
    arrow_reader& operator=(arrow_reader const& other) = delete;
    arrow_reader(arrow_reader&& other) noexcept;
    arrow_reader& operator=(arrow_reader&& other) noexcept;

    /**
     * @brief read the parquet record
     * @param ref [out] the record reference filled with the parquet data
     * @return true when successful
     * @return false otherwise
     */
    bool next(accessor::record_ref& ref) override;

    /**
     * @brief close the reader
     * @return true when successful
     * @return false otherwise
     */
    bool close() override;

    /**
     * @brief accessor to the read file path
     */
    [[nodiscard]] std::string path() const noexcept override;

    /**
     * @brief accessor to the number of successful read
     */
    [[nodiscard]] std::size_t read_count() const noexcept override;

    /**
     * @brief accessor to the metadata derived from the parquet schema definition
     */
    [[nodiscard]] maybe_shared_ptr<meta::external_record_meta> const& meta() override;

    /**
     * @brief accessor to the row group count
     */
    [[nodiscard]] std::size_t row_group_count() const noexcept override;

    /**
     * @brief accessor to the size in bytes of the current record batch
     * @return the size in bytes of the current record batch on success
     * @return std::nullopt if the size cannot be determined
     */
    [[nodiscard]] std::optional<std::size_t> record_batch_size() const noexcept;

    /**
     * @brief factory function to construct the new arrow_reader object
     * @param path the path to the pqrquet file to read
     * @param opt the options for reader
     * @param row_group_index the 0-origin index specifying the row group to read. Specify `undefined` to read the first
     * row group in the file.
     * @return newly created object on success
     * @return nullptr otherwise
     */
    static std::shared_ptr<arrow_reader> open(
        std::string_view path,
        reader_option const* opt = nullptr,
        std::size_t row_group_index = index_unspecified
    );

private:
    class impl;
    std::unique_ptr<impl> impl_;
};

}  // namespace jogasaki::executor::file
