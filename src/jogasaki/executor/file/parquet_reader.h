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
#include <string_view>

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

constexpr std::size_t npos = static_cast<std::size_t>(-1);

/**
 * @brief field locator indicates what parquet column (by name or index) is used as source to read
 */
struct parquet_reader_field_locator {
    parquet_reader_field_locator() = default;

    parquet_reader_field_locator(std::string_view name, std::size_t index) noexcept :
        name_(name),
        index_(index),
        empty_(false)
    {}
    std::string name_{};
    std::size_t index_{npos};
    bool empty_{true};
};

class parquet_reader_option {
public:
    parquet_reader_option() = default;

    /**
     * @brief create new option
     * @param loc locators indicating source to read. The order must correspond to the field order in `meta`
     * @param meta metadata of the record_ref that reader's next() writes data to.
     */
    parquet_reader_option(
        std::vector<parquet_reader_field_locator> loc,
        meta::record_meta const& meta
    ) noexcept :
        loc_(std::move(loc)),
        meta_(std::addressof(meta))
    {
        BOOST_ASSERT(loc_.size() ==  meta_->field_count());  //NOLINT
    }

    std::vector<parquet_reader_field_locator> loc_{};
    meta::record_meta const* meta_{};
};

/**
 * @brief parquet file reader
 * @details this reader is created with mapping from parquet fields to record_ref fields that represent values for
 * parameters/placeholders. The reader reads the parquet record and fills the fields according to the mapping.
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
     * @brief read the parquet record
     * @param ref [out] the record reference filled with the parquet data
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
     * @brief accessor to the number of successful read
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
    static std::shared_ptr<parquet_reader> open(std::string_view path, parquet_reader_option const* opt = nullptr);

private:
    maybe_shared_ptr<meta::external_record_meta> meta_{};
    maybe_shared_ptr<meta::record_meta const> parameter_meta_{};
    std::unique_ptr<parquet::ParquetFileReader> file_reader_{};
    std::shared_ptr<parquet::RowGroupReader> row_group_reader_{};
    std::vector<std::shared_ptr<parquet::ColumnReader>> column_readers_{};
    std::vector<parquet::ColumnDescriptor const*> columns_{};

    boost::filesystem::path path_{};
    std::size_t read_count_{};

    data::aligned_buffer buf_{};

    std::vector<std::size_t> parameter_to_parquet_field_{};

    bool init(std::string_view path, parquet_reader_option const* opt);
};


}
