/*
 * Copyright 2018-2020 tsurugi project.
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

#include <iomanip>

#include <boost/filesystem.hpp>
#include <arrow/io/file.h>
#include <arrow/util/logging.h>
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/meta/external_record_meta.h>
#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/data/aligned_buffer.h>

namespace jogasaki::executor::file {

using takatori::util::maybe_shared_ptr;

/**
 * @brief parquet file writer
 */
class parquet_reader {
public:
    /**
     * @brief create new object
     * @details this function is intended to be called from open(). Use open() function because it can report error
     * during initialization.
     */
    parquet_reader() = default;

    /**
     * @brief write the record
     * @param ref [out] the record reference read by the writer
     * @return true when successful
     * @return false otherwise
     */
    bool next(accessor::record_ref& ref);

    /**
     * @brief close the reader
     * @return true when successful
     * @return false otherwise
     */
    bool close();

    /**
     * @brief accessor to the read file path
     */
    [[nodiscard]] std::string path() const noexcept;

    /**
     * @brief accessor to the number of successful write
     */
    [[nodiscard]] std::size_t read_count() const noexcept;

    /**
     * @brief accessor to the metadata derived from the parquet schema definition
     */
    [[nodiscard]] maybe_shared_ptr<meta::external_record_meta> const& meta();

    /**
     * @brief factory function to construct the new parquet_writer object
     * @param meta metadata of the written records
     * @param path the file path that is to be written
     * @return newly created object on success
     * @return nullptr otherwise
     */
    static std::shared_ptr<parquet_reader> open(std::string_view path);

private:
    maybe_shared_ptr<meta::external_record_meta> meta_{};
    std::unique_ptr<parquet::ParquetFileReader> file_reader_{};
    std::shared_ptr<parquet::RowGroupReader> row_group_reader_{};
    std::vector<std::shared_ptr<parquet::ColumnReader>> column_readers_{};

    boost::filesystem::path path_{};
    std::size_t read_count_{};

    std::unordered_map<std::string, std::size_t> column_name_to_index_{};
    std::unordered_map<std::size_t, std::size_t> external_to_parquet_column_index_{};

    data::aligned_buffer buf_{};

    bool init(std::string_view path);
};


}
