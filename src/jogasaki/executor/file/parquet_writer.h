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

#include <iomanip>
#include <arrow/io/file.h>
#include <arrow/util/logging.h>
#include <boost/filesystem.hpp>
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/executor/file/file_writer.h>
#include <jogasaki/meta/external_record_meta.h>

#include "column_option.h"

namespace jogasaki::executor::file {

using takatori::util::maybe_shared_ptr;

/**
 * @brief parquet file writer
 */
class parquet_writer : public file_writer {
public:
    /**
     * @brief create empty object
     */
    parquet_writer() = default;

    parquet_writer(parquet_writer const& other) = delete;
    parquet_writer& operator=(parquet_writer const& other) = delete;
    parquet_writer(parquet_writer&& other) noexcept = default;
    parquet_writer& operator=(parquet_writer&& other) noexcept = default;

    /**
     * @brief create new object
     * @param meta record meta with column names
     * @details this function is intended to be called from open(). Use open() function because it can report error
     * during initialization.
     */
    explicit parquet_writer(maybe_shared_ptr<meta::external_record_meta> meta);

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
     * @param path the file path that is to be written
     * @return newly created object on success
     * @return nullptr otherwise
     */
    static std::shared_ptr<parquet_writer>
    open(maybe_shared_ptr<meta::external_record_meta> meta, std::string_view path);

private:
    maybe_shared_ptr<meta::external_record_meta> meta_{};
    std::shared_ptr<::arrow::io::FileOutputStream> fs_{};
    std::shared_ptr<parquet::ParquetFileWriter> file_writer_{};
    parquet::RowGroupWriter* row_group_writer_{};
    std::vector<parquet::ColumnWriter*> column_writers_{};
    boost::filesystem::path path_{};
    std::size_t write_count_{};
    std::vector<details::column_option> column_options_{};

    std::pair<std::shared_ptr<parquet::schema::GroupNode>, std::vector<details::column_option>> create_schema();
    bool write_int4(std::size_t colidx, std::int32_t v, bool null = false);
    bool write_int8(std::size_t colidx, std::int64_t v, bool null = false);
    bool write_float4(std::size_t colidx, float v, bool null = false);
    bool write_float8(std::size_t colidx, double v, bool null = false);
    bool write_character(std::size_t colidx, accessor::text v, bool null = false);
    bool write_decimal(
        std::size_t colidx,
        runtime_t<meta::field_type_kind::decimal> v,
        bool null = false,
        details::column_option const& colopt = {}
    );
    bool write_date(std::size_t colidx, runtime_t<meta::field_type_kind::date> v, bool null = false);
    bool write_time_of_day(std::size_t colidx, runtime_t<meta::field_type_kind::time_of_day> v, bool null = false);
    bool write_time_point(std::size_t colidx, runtime_t<meta::field_type_kind::time_point> v, bool null = false);
    bool init(std::string_view path);
};

}  // namespace jogasaki::executor::file
