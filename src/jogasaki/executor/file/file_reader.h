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
#include <string_view>
#include <boost/filesystem.hpp>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/data/aligned_buffer.h>
#include <jogasaki/meta/external_record_meta.h>

namespace jogasaki::executor::file {

using takatori::util::maybe_shared_ptr;

constexpr std::size_t npos = static_cast<std::size_t>(-1);

/**
 * @brief field locator indicates what parquet column (by name or index) is used as source to read
 */
struct reader_field_locator {
    reader_field_locator() = default;

    reader_field_locator(std::string_view name, std::size_t index) noexcept :
        name_(name),
        index_(index),
        empty_(false)
    {}
    std::string name_{};  //NOLINT
    std::size_t index_{npos};  //NOLINT
    bool empty_{true};  //NOLINT
};

class reader_option {
public:

    reader_option() = default;

    /**
     * @brief create new option
     * @param loc locators indicating source to read. The order must correspond to the field order in `meta`
     * @param meta metadata of the record_ref that reader's next() writes data to.
     */
    reader_option(
        std::vector<reader_field_locator> loc,
        meta::record_meta const& meta
    ) noexcept :
        loc_(std::move(loc)),
        meta_(std::addressof(meta))
    {
        BOOST_ASSERT(loc_.size() ==  meta_->field_count());  //NOLINT
    }

    std::vector<reader_field_locator> loc_{};  //NOLINT
    meta::record_meta const* meta_{};  //NOLINT
};

/**
 * @brief file reader interface
 * @details this reader is created with mapping from parquet fields to record_ref fields that represent values for
 */
class file_reader {
public:
    static constexpr std::size_t index_unspecified = static_cast<std::size_t>(-1);

    /**
     * @brief create new object
     * @details this function is intended to be called from open(). Use open() function because it can report error
     * during initialization.
     */
    file_reader() = default;

    /**
     * @brief destruct object
     * @details destruct the object closing the file if any opened
     */
    virtual ~file_reader() = default;

    file_reader(file_reader const& other) = default;
    file_reader& operator=(file_reader const& other) = default;
    file_reader(file_reader&& other) noexcept = default;
    file_reader& operator=(file_reader&& other) noexcept = default;

    /**
     * @brief read the parquet record
     * @param ref [out] the record reference filled with the parquet data
     * @return true when successful
     * @return false otherwise
     */
    virtual bool next(accessor::record_ref& ref) = 0;

    /**
     * @brief close the reader
     * @return true when successful
     * @return false otherwise
     */
    virtual bool close() = 0;

    /**
     * @brief accessor to the read file path
     */
    [[nodiscard]] virtual std::string path() const noexcept = 0;

    /**
     * @brief accessor to the number of successful read
     */
    [[nodiscard]] virtual std::size_t read_count() const noexcept = 0;

    /**
     * @brief accessor to the metadata derived from the parquet schema definition
     */
    [[nodiscard]] virtual maybe_shared_ptr<meta::external_record_meta> const& meta() = 0;

    /**
     * @brief accessor to the row group count
     */
    [[nodiscard]] virtual std::size_t row_group_count() const noexcept = 0;

};

}  // namespace jogasaki::executor::file
