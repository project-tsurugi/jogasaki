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

#include <limits>
#include <cstdint>
#include <string_view>

namespace jogasaki {

/**
 * @brief default number of partitions
 */
constexpr std::size_t default_partitions = 5;

/**
 * @brief number of bits per byte
 * @details we assume byte and char are always 8 bits long
 */
constexpr static std::size_t bits_per_byte = std::numeric_limits<std::uint8_t>::digits;

/**
 * @brief system table name for sequences
 * @details the table name for the sequence id mapping
 */
constexpr static std::string_view system_sequences_name = "system_sequences";

/**
 * @brief average number of indices per table
 * @details the count includes primary/secondary.
 * This is expected value for typical use case. Should be used for estimation, not limitation.
 */
constexpr static std::size_t approx_index_count_per_table = 5;

/**
 * @brief default size in bytes for record buffer
 * @details Used to prepare the write buffer for record.
 * This is expected value for typical use case. Should be used for estimation, not limitation.
 */
constexpr std::size_t default_record_buffer_size = 1024;

}
