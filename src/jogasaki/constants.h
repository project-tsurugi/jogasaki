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

#include <limits>
#include <cstdint>
#include <string_view>

namespace jogasaki {

/**
 * @brief current message version (major)
 */
constexpr std::size_t service_message_version_major = 1;

/**
 * @brief current message version (minor)
 */
constexpr std::size_t service_message_version_minor = 0;

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
 * @brief old system table name for sequences
 * @details the deprecated table name for sequences
 */
constexpr static std::string_view legacy_system_sequences_name = "system_sequences";

/**
 * @brief system table name for sequences
 * @details the table name for the sequence id mapping
 */
constexpr static std::string_view system_sequences_name = "__system_sequences";

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

/**
 * @brief prefix used for generated primary key column name and its sequence
 */
constexpr static std::string_view generated_pkey_column_prefix = "__generated_rowid_";

/**
 * @brief default precision for decimal
 * @details default precision for decimal types that has no precision information defined (e.g. DECIMAL(*, 0))
 */
constexpr std::size_t decimal_default_precision = 38;

/**
 * @brief default precision for scale for dump
 * @details default precision for decimal types that has no scale information defined (e.g. DECIMAL(*, *))
 */
constexpr std::size_t dumped_decimal_default_scale = 5;

/**
 * @brief serialized storage metadata version
 */
constexpr std::size_t metadata_format_version = 10;

/**
 * @brief format id of the explain response
 */
constexpr std::string_view sql_proto_explain_format_id{"jogasaki-statement.json"};

/**
 * @brief format version of the explain response
 */
constexpr std::size_t sql_proto_explain_format_version = 1;
}
