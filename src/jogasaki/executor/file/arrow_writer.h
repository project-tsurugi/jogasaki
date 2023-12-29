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

#include <boost/filesystem.hpp>
#include <arrow/io/file.h>
#include <arrow/util/logging.h>

#include <arrow/ipc/writer.h>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/meta/external_record_meta.h>
#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/executor/file/file_writer.h>

#include "column_option.h"

namespace jogasaki::executor::file {

using takatori::util::maybe_shared_ptr;

/**
 * @brief arrow file writer
 */
class arrow_writer : public file_writer {
public:
    /**
     * @brief create empty object
     */
    arrow_writer() = default;

    arrow_writer(arrow_writer const& other) = delete;
    arrow_writer& operator=(arrow_writer const& other) = delete;
    arrow_writer(arrow_writer&& other) noexcept = default;
    arrow_writer& operator=(arrow_writer&& other) noexcept = default;

    /**
     * @brief create new object
     * @param meta record meta with column names
     * @details this function is intended to be called from open(). Use open() function because it can report error
     * during initialization.
     */
    explicit arrow_writer(maybe_shared_ptr<meta::external_record_meta> meta);

    /**
     * @brief destruct object
     * @details destruct the object closing the file if any opened
     */
    ~arrow_writer() noexcept override;

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
     * @brief factory function to construct the new arrow_writer object
     * @param meta metadata of the written records
     * @param path the file path that is to be written
     * @return newly created object on success
     * @return nullptr otherwise
     */
    static std::shared_ptr<arrow_writer> open(maybe_shared_ptr<meta::external_record_meta> meta, std::string_view path);

private:
    maybe_shared_ptr<meta::external_record_meta> meta_{};
    std::shared_ptr<::arrow::io::FileOutputStream> fs_{};
    std::shared_ptr<arrow::ipc::RecordBatchWriter> record_batch_writer_{};
    std::shared_ptr<arrow::Schema> schema_{};

    std::vector<std::shared_ptr<arrow::ArrayBuilder>> array_builders_{};
    std::vector<std::shared_ptr<arrow::Array>> arrays_{};
    boost::filesystem::path path_{};
    std::size_t write_count_{};
    std::vector<details::column_option> column_options_{};

    std::pair<std::shared_ptr<arrow::Schema>, std::vector<details::column_option>> create_schema();
    bool write_int1(std::size_t colidx, std::int32_t v);
    bool write_int2(std::size_t colidx, std::int32_t v);
    bool write_int4(std::size_t colidx, std::int32_t v);
    bool write_int8(std::size_t colidx, std::int64_t v);
    bool write_float4(std::size_t colidx, float v);
    bool write_float8(std::size_t colidx, double v);
    bool write_character(std::size_t colidx, accessor::text v, details::column_option const& colopt);
    bool write_decimal(std::size_t colidx, runtime_t<meta::field_type_kind::decimal> v, details::column_option const& colopt = {});
    bool write_date(std::size_t colidx, runtime_t<meta::field_type_kind::date> v);
    bool write_time_of_day(std::size_t colidx, runtime_t<meta::field_type_kind::time_of_day> v);
    bool write_time_point(std::size_t colidx, runtime_t<meta::field_type_kind::time_point> v);
    bool init(std::string_view path);
    void finish();
};

}
