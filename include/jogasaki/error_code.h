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

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <string_view>

namespace jogasaki {

/**
 * @brief error code
 */
enum class error_code : std::int64_t {
    none = 0,
    sql_service_exception = 1000,
    sql_execution_exception = 2000,
    constraint_violation_exception = 2001,
    unique_constraint_violation_exception = 2002,
    not_null_constraint_violation_exception = 2003,
    referential_integrity_constraint_violation_exception = 2004,
    check_constraint_violation_exception = 2005,
    evaluation_exception = 2010,
    value_evaluation_exception = 2011,
    scalar_subquery_evaluation_exception = 2012,
    target_not_found_exception = 2014,
    target_already_exists_exception = 2016,
    inconsistent_statement_exception = 2018,
    restricted_operation_exception = 2020,
    dependencies_violation_exception = 2021,
    write_operation_by_rtx_exception = 2022,
    ltx_write_operation_without_write_preserve_exception = 2023,
    read_operation_on_restricted_read_area_exception = 2024,
    inactive_transaction_exception = 2025,
    parameter_exception = 2027,
    unresolved_placeholder_exception = 2028,
    load_file_exception = 2030,
    load_file_not_found_exception = 2031,
    load_file_format_exception = 2032,
    dump_file_exception = 2033,
    dump_directory_inaccessible_exception = 2034,
    sql_limit_reached_exception = 2036,
    transaction_exceeded_limit_exception = 2037,

    sql_request_timeout_exception = 2039,
    data_corruption_exception = 2041,
    secondary_index_corruption_exception = 2042,
    request_failure_exception = 2044,
    transaction_not_found_exception = 2045,
    statement_not_found_exception = 2046,
    internal_exception = 2048,
    unsupported_runtime_feature_exception = 2050,
    blocked_by_high_priority_transaction_exception = 2052,
    invalid_runtime_value_exception = 2054,
    value_out_of_range_exception = 2056,
    value_too_long_exception = 2058,
    invalid_decimal_value_exception = 2060,

    compile_exception = 3000,
    syntax_exception = 3001,
    analyze_exception = 3002,
    type_analyze_exception = 3003,
    symbol_analyze_exception = 3004,
    value_analyze_exception = 3005,
    unsupported_compiler_feature_exception = 3010,

    cc_exception = 4000,
    occ_exception = 4001,
    occ_read_exception = 4010,
    conflict_on_write_preserve_exception = 4015,
    occ_write_exception = 4011,
    ltx_exception = 4003,
    ltx_read_exception = 4013,
    ltx_write_exception = 4014,
    rtx_exception = 4005,
    blocked_by_concurrent_operation_exception = 4007,

    // internal use
    request_canceled = 50011,
    lob_file_io_error = 50012,
    lob_reference_invalid = 50013,
    operation_denied = 50014,
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponding string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(error_code value) noexcept {
    using namespace std::string_view_literals;
    using code = error_code;
    switch (value) {
        case code::none: return "none"sv;
        case code::sql_service_exception: return "sql_service_exception"sv;
        case code::sql_execution_exception: return "sql_execution_exception"sv;
        case code::constraint_violation_exception: return "constraint_violation_exception"sv;
        case code::unique_constraint_violation_exception: return "unique_constraint_violation_exception"sv;
        case code::not_null_constraint_violation_exception: return "not_null_constraint_violation_exception"sv;
        case code::referential_integrity_constraint_violation_exception: return "referential_integrity_constraint_violation_exception"sv;
        case code::check_constraint_violation_exception: return "check_constraint_violation_exception"sv;
        case code::evaluation_exception: return "evaluation_exception"sv;
        case code::value_evaluation_exception: return "value_evaluation_exception"sv;
        case code::scalar_subquery_evaluation_exception: return "scalar_subquery_evaluation_exception"sv;
        case code::target_not_found_exception: return "target_not_found_exception"sv;
        case code::target_already_exists_exception: return "target_already_exists_exception"sv;
        case code::inconsistent_statement_exception: return "inconsistent_statement_exception"sv;
        case code::restricted_operation_exception: return "restricted_operation_exception"sv;
        case code::dependencies_violation_exception: return "dependencies_violation_exception"sv;
        case code::write_operation_by_rtx_exception: return "write_operation_by_rtx_exception"sv;
        case code::ltx_write_operation_without_write_preserve_exception: return "ltx_write_operation_without_write_preserve_exception"sv;
        case code::read_operation_on_restricted_read_area_exception: return "read_operation_on_restricted_read_area_exception"sv;
        case code::inactive_transaction_exception: return "inactive_transaction_exception"sv;
        case code::parameter_exception: return "parameter_exception"sv;
        case code::unresolved_placeholder_exception: return "unresolved_placeholder_exception"sv;
        case code::load_file_exception: return "load_file_exception"sv;
        case code::load_file_not_found_exception: return "load_file_not_found_exception"sv;
        case code::load_file_format_exception: return "load_file_format_exception"sv;
        case code::dump_file_exception: return "dump_file_exception"sv;
        case code::dump_directory_inaccessible_exception: return "dump_directory_inaccessible_exception"sv;
        case code::sql_limit_reached_exception: return "sql_limit_reached_exception"sv;
        case code::transaction_exceeded_limit_exception: return "transaction_exceeded_limit_exception"sv;
        case code::sql_request_timeout_exception: return "sql_request_timeout_exception"sv;
        case code::data_corruption_exception: return "data_corruption_exception"sv;
        case code::secondary_index_corruption_exception: return "secondary_index_corruption_exception"sv;
        case code::request_failure_exception: return "request_failure_exception"sv;
        case code::transaction_not_found_exception: return "transaction_not_found_exception"sv;
        case code::statement_not_found_exception: return "statement_not_found_exception"sv;
        case code::internal_exception: return "internal_exception"sv;
        case code::unsupported_runtime_feature_exception: return "unsupported_runtime_feature_exception"sv;
        case code::blocked_by_high_priority_transaction_exception: return "blocked_by_high_priority_transaction_exception"sv;
        case code::invalid_runtime_value_exception: return "invalid_runtime_value_exception"sv;
        case code::value_out_of_range_exception: return "value_out_of_range_exception"sv;
        case code::value_too_long_exception: return "value_too_long_exception"sv;
        case code::invalid_decimal_value_exception: return "invalid_decimal_value_exception"sv;
        case code::compile_exception: return "compile_exception"sv;
        case code::syntax_exception: return "syntax_exception"sv;
        case code::analyze_exception: return "analyze_exception"sv;
        case code::type_analyze_exception: return "type_analyze_exception"sv;
        case code::symbol_analyze_exception: return "symbol_analyze_exception"sv;
        case code::value_analyze_exception: return "value_analyze_exception"sv;
        case code::unsupported_compiler_feature_exception: return "unsupported_compiler_feature_exception"sv;
        case code::cc_exception: return "cc_exception"sv;
        case code::occ_exception: return "occ_exception"sv;
        case code::occ_read_exception: return "occ_read_exception"sv;
        case code::conflict_on_write_preserve_exception: return "conflict_on_write_preserve_exception"sv;
        case code::occ_write_exception: return "occ_write_exception"sv;
        case code::ltx_exception: return "ltx_exception"sv;
        case code::ltx_read_exception: return "ltx_read_exception"sv;
        case code::ltx_write_exception: return "ltx_write_exception"sv;
        case code::rtx_exception: return "rtx_exception"sv;
        case code::blocked_by_concurrent_operation_exception: return "blocked_by_concurrent_operation_exception"sv;
        case code::request_canceled: return "request_canceled"sv;
        case code::lob_file_io_error: return "lob_file_io_error"sv;
        case code::lob_reference_invalid: return "lob_reference_invalid"sv;
        case code::operation_denied: return "operation_denied"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, error_code value) {
    return out << to_string_view(value);
}

}  // namespace jogasaki
