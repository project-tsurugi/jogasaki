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
#include <iomanip>
#include <memory>
#include <string>
#include <string_view>
#include <vector>
#include <arrow/io/file.h>
#include <arrow/ipc/reader.h>
#include <arrow/record_batch.h>
#include <arrow/util/logging.h>
#include <boost/filesystem.hpp>
#include <boost/filesystem/path.hpp>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/data/aligned_buffer.h>
#include <jogasaki/executor/file/file_reader.h>
#include <jogasaki/meta/external_record_meta.h>
#include <jogasaki/meta/record_meta.h>

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
    arrow_reader() = default;

    /**
     * @brief destruct object
     * @details destruct the object closing the file if any opened
     */
    ~arrow_reader() noexcept override;

    arrow_reader(arrow_reader const& other) = delete;
    arrow_reader& operator=(arrow_reader const& other) = delete;
    arrow_reader(arrow_reader&& other) noexcept = default;
    arrow_reader& operator=(arrow_reader&& other) noexcept = default;

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
     * @brief accessor to the current record batch
     */
    [[nodiscard]] std::shared_ptr<arrow::RecordBatch> const& record_batch() const noexcept;

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
    maybe_shared_ptr<meta::external_record_meta> meta_{};
    maybe_shared_ptr<meta::record_meta const> parameter_meta_{};

    std::shared_ptr<arrow::io::ReadableFile> input_file_{};
    std::shared_ptr<arrow::ipc::RecordBatchFileReader> file_reader_{};
    std::shared_ptr<arrow::RecordBatch> record_batch_{};

    // std::vector<parquet::ColumnDescriptor const*> columns_{};
    boost::filesystem::path path_{};
    std::size_t read_count_{};
    data::aligned_buffer buf_{};
    std::vector<std::size_t> parameter_to_field_{};
    std::size_t row_group_count_{};
    std::size_t row_group_index_{};

    std::size_t offset_{};

    bool init(std::string_view path, reader_option const* opt, std::size_t row_group_index);
};

}  // namespace jogasaki::executor::file
