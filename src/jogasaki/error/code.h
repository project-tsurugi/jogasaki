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

#include <cstddef>
#include <cstdlib>
#include <string_view>
#include <ostream>

namespace jogasaki::error {

/**
 * @brief error code
 */
enum class code : std::int64_t {
    none = 0,
    sql_service_exception = 1,
    sql_execution_exception = 2,
    constraint_violation_exception = 3,
    unique_constraint_violation_exception = 4,
    not_null_constraint_violation_exception = 5,
    referential_integrity_constraint_violation_exception = 6,
    check_constraint_violation_exception = 7,
    evaluation_exception = 8,
    value_evaluation_exception = 9,
    scalar_subquery_evaluation_exception = 10,
    target_not_found_exception = 11,
    target_already_exists_exception = 12,
    inconsistent_statement_exception = 13,
    restricted_operation_exception = 14,
    dependencies_violation_exception = 15,
    write_operation_by_rtx_exception = 16,
    ltx_write_operation_without_write_preserve_exception = 17,
    read_operation_on_restricted_read_area_exception = 18,
    inactive_transaction_exception = 19,
    parameter_exception = 20,
    unresolved_placeholder_exception = 21,
    load_file_ioexception = 22,
    load_file_not_found_exception = 23,
    load_file_format_exception = 24,
    dump_file_ioexception = 25,
    dump_directory_inaccessible_exception = 26,
    sql_limit_reached_exception = 27,
    transaction_exceeded_limit_exception = 28,
    sql_request_timed_out_exception = 29,
    data_corruption_exception = 30,
    secondary_index_corruption_exception = 31,
    request_failure_exception = 32,
    transaction_not_found_exception = 33,
    statement_not_found_exception = 34,
    internal_exception = 35,
    unsupported_runtime_feature_exception = 36,
    blocked_by_high_priority_transaction_exception = 37,
    compile_exception = 101,
    syntax_exception = 102,
    analyze_exception = 103,
    type_analyze_exception = 104,
    symbol_analyze_exception = 105,
    value_analyze_exception = 106,
    unsupported_compiler_feature_exception = 107,
    cc_exception = 201,
    occ_exception = 202,
    occ_read_exception = 203,
    conflict_on_write_preserve_exception = 204,
    occ_write_exception = 205,
    ltx_exception = 206,
    ltx_read_exception = 207,
    ltx_write_exception = 208,
    rtx_exception = 209,
    blocked_by_concurrent_operation_exception = 210,
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponded string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(code value) noexcept {
    using namespace std::string_view_literals;
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
        case code::load_file_ioexception: return "load_file_ioexception"sv;
        case code::load_file_not_found_exception: return "load_file_not_found_exception"sv;
        case code::load_file_format_exception: return "load_file_format_exception"sv;
        case code::dump_file_ioexception: return "dump_file_ioexception"sv;
        case code::dump_directory_inaccessible_exception: return "dump_directory_inaccessible_exception"sv;
        case code::sql_limit_reached_exception: return "sql_limit_reached_exception"sv;
        case code::transaction_exceeded_limit_exception: return "transaction_exceeded_limit_exception"sv;
        case code::sql_request_timed_out_exception: return "sql_request_timed_out_exception"sv;
        case code::data_corruption_exception: return "data_corruption_exception"sv;
        case code::secondary_index_corruption_exception: return "secondary_index_corruption_exception"sv;
        case code::request_failure_exception: return "request_failure_exception"sv;
        case code::transaction_not_found_exception: return "transaction_not_found_exception"sv;
        case code::statement_not_found_exception: return "statement_not_found_exception"sv;
        case code::internal_exception: return "internal_exception"sv;
        case code::unsupported_runtime_feature_exception: return "unsupported_runtime_feature_exception"sv;
        case code::blocked_by_high_priority_transaction_exception: return "blocked_by_high_priority_transaction_exception"sv;
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
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, code value) {
    return out << to_string_view(value);
}

}
