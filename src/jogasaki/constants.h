/*
 * Copyright 2018-2025 Project Tsurugi.
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
constexpr std::size_t service_message_version_major = 2;

/**
 * @brief current message version (minor)
 */
constexpr std::size_t service_message_version_minor = 0;

/**
 * @brief default number of partitions for testing and mocking purposes.
 *
 * @details this constant is intended solely for use in test and mock scenarios.
 * For production code, retrieve the default number of partitions using
 * the appropriate configuration method, such as `configuration::default_partitions()`.
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
 * @brief prefix used for generated primary key column name and its sequence
 */
constexpr static std::string_view generated_sequence_name_prefix = "__generated_sequence_";

/**
 * @brief max precision for decimal
 * @details max precision allowed for decimal types
 */
constexpr std::size_t decimal_max_precision = 38;

/**
 * @brief min precision for decimal
 * @details min precision allowed for decimal types
 */
constexpr std::size_t decimal_min_precision = 1;

/**
 * @brief default precision for decimal
 * @details default precision for decimal types that has no precision information defined (e.g. DECIMAL(*, 0) in column definition)
 */
constexpr std::size_t decimal_default_precision = decimal_max_precision;

/**
 * @brief default precision for decimal
 * @details default precision for decimal types that has no precision/scale information provided at all (e.g. DECIMAL without parenthesis)
 */
constexpr std::size_t decimal_default_precision_no_parenthesis = decimal_max_precision;

/**
 * @brief max length for varchar/char types that are part of primary/secondary indices key
 */
constexpr std::size_t character_type_max_length_for_key = 30716;

/**
 * @brief max length for varchar/char types that are not part of primary/secondary indices key
 */
constexpr std::size_t character_type_max_length_for_value = 2UL * 1024UL * 1024UL - 20UL;

/**
 * @brief default length for char type
 */
constexpr std::size_t character_type_default_length = 1;

/**
 * @brief max length for varbinary/binary types that are part of primary/secondary indices key
 */
constexpr std::size_t octet_type_max_length_for_key = 30716;

/**
 * @brief max length for varbinary/binary types that are not part of primary/secondary indices key
 */
constexpr std::size_t octet_type_max_length_for_value = 2UL * 1024UL * 1024UL - 20UL;

/**
 * @brief max length for varbinary/binary types
 */
constexpr std::size_t octet_type_default_length = 1;

/**
 * @brief default precision for scale for dump
 * @details default precision for decimal types that has no scale information defined (e.g. DECIMAL(*, *))
 */
constexpr std::size_t dumped_decimal_default_scale = 5;

/**
 * @brief serialized storage metadata version
 */
constexpr std::size_t metadata_format_version = 12;

/**
 * @brief storage metadata version minimum acceptable by the current code
 * @note this value must be equals to or less than metadata_format_version
 */
constexpr std::size_t compatible_metadata_format_version = 10;

/**
 * @brief format id of the explain response
 */
constexpr std::string_view sql_proto_explain_format_id{"jogasaki-statement.json"};

/**
 * @brief format version of the explain response
 */
constexpr std::size_t sql_proto_explain_format_version = 1;

/**
 * @brief catalog name
 * @details the name for the built-in database catalog
 */
constexpr static std::string_view default_catalog_name = "";  //NOLINT(readability-redundant-string-init)

/**
 * @brief public schema name
 * @details the name for the built-in public schema
 */
constexpr static std::string_view public_schema_name = "public";

/**
 * @brief storage namespace for public schema
 * @details the namespace identifier of the storage corresponding to the public schema
 */
constexpr static std::string_view storage_namespace_for_public_schema = "";  //NOLINT(readability-redundant-string-init)

/**
 * @brief prefix used for system internal identifiers
 */
constexpr static std::string_view system_identifier_prefix = "__";

/**
 * @brief initial sequence version
 */
constexpr std::size_t initial_sequence_version = 1;

/**
 * @brief session variable name to enable execution plan output
 * @details the name for the session variable to enable execution plan output for external logging
 */
constexpr static std::string_view session_variable_sql_plan_recording = "sql.plan_recording";

/**
 * @brief transaction store identifier used in session store
 */
constexpr std::size_t session_store_element_id_transactions = 10;

/**
 * @brief statement store identifier used in session store
 */
constexpr std::size_t session_store_element_id_prepared_statements = 11;

}  // namespace jogasaki
