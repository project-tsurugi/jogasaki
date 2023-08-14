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

#include <string_view>
#include <atomic>
#include <memory>

#include <jogasaki/error_code.h>

#include "jogasaki/proto/sql/error.pb.h"

namespace jogasaki::api::impl {

using takatori::util::unsafe_downcast;
using takatori::util::fail;

namespace sql = jogasaki::proto::sql;

inline sql::error::Code map_error(jogasaki::error_code s) {
    switch (s) {
        case jogasaki::error_code::sql_service_exception: return sql::error::SQL_SERVICE_EXCEPTION;
        case jogasaki::error_code::sql_execution_exception: return sql::error::SQL_EXECUTION_EXCEPTION;
        case jogasaki::error_code::constraint_violation_exception: return sql::error::CONSTRAINT_VIOLATION_EXCEPTION;
        case jogasaki::error_code::unique_constraint_violation_exception: return sql::error::UNIQUE_CONSTRAINT_VIOLATION_EXCEPTION;
        case jogasaki::error_code::not_null_constraint_violation_exception: return sql::error::NOT_NULL_CONSTRAINT_VIOLATION_EXCEPTION;
        case jogasaki::error_code::referential_integrity_constraint_violation_exception: return sql::error::REFERENTIAL_INTEGRITY_CONSTRAINT_VIOLATION_EXCEPTION;
        case jogasaki::error_code::check_constraint_violation_exception: return sql::error::CHECK_CONSTRAINT_VIOLATION_EXCEPTION;
        case jogasaki::error_code::evaluation_exception: return sql::error::EVALUATION_EXCEPTION;
        case jogasaki::error_code::value_evaluation_exception: return sql::error::VALUE_EVALUATION_EXCEPTION;
        case jogasaki::error_code::scalar_subquery_evaluation_exception: return sql::error::SCALAR_SUBQUERY_EVALUATION_EXCEPTION;
        case jogasaki::error_code::target_not_found_exception: return sql::error::TARGET_NOT_FOUND_EXCEPTION;
        case jogasaki::error_code::target_already_exists_exception: return sql::error::TARGET_ALREADY_EXISTS_EXCEPTION;
        case jogasaki::error_code::inconsistent_statement_exception: return sql::error::INCONSISTENT_STATEMENT_EXCEPTION;
        case jogasaki::error_code::restricted_operation_exception: return sql::error::RESTRICTED_OPERATION_EXCEPTION;
        case jogasaki::error_code::dependencies_violation_exception: return sql::error::DEPENDENCIES_VIOLATION_EXCEPTION;
        case jogasaki::error_code::write_operation_by_rtx_exception: return sql::error::WRITE_OPERATION_BY_RTX_EXCEPTION;
        case jogasaki::error_code::ltx_write_operation_without_write_preserve_exception: return sql::error::LTX_WRITE_OPERATION_WITHOUT_WRITE_PRESERVE_EXCEPTION;
        case jogasaki::error_code::read_operation_on_restricted_read_area_exception: return sql::error::READ_OPERATION_ON_RESTRICTED_READ_AREA_EXCEPTION;
        case jogasaki::error_code::inactive_transaction_exception: return sql::error::INACTIVE_TRANSACTION_EXCEPTION;
        case jogasaki::error_code::parameter_exception: return sql::error::PARAMETER_EXCEPTION;
        case jogasaki::error_code::unresolved_placeholder_exception: return sql::error::UNRESOLVED_PLACEHOLDER_EXCEPTION;
        case jogasaki::error_code::load_file_ioexception: return sql::error::LOAD_FILE_IOEXCEPTION;
        case jogasaki::error_code::load_file_not_found_exception: return sql::error::LOAD_FILE_NOT_FOUND_EXCEPTION;
        case jogasaki::error_code::load_file_format_exception: return sql::error::LOAD_FILE_FORMAT_EXCEPTION;
        case jogasaki::error_code::dump_file_ioexception: return sql::error::DUMP_FILE_IOEXCEPTION;
        case jogasaki::error_code::dump_directory_inaccessible_exception: return sql::error::DUMP_DIRECTORY_INACCESSIBLE_EXCEPTION;
        case jogasaki::error_code::sql_limit_reached_exception: return sql::error::SQL_LIMIT_REACHED_EXCEPTION;
        case jogasaki::error_code::transaction_exceeded_limit_exception: return sql::error::TRANSACTION_EXCEEDED_LIMIT_EXCEPTION;
        case jogasaki::error_code::sql_request_timed_out_exception: return sql::error::SQL_REQUEST_TIMED_OUT_EXCEPTION;
        case jogasaki::error_code::data_corruption_exception: return sql::error::DATA_CORRUPTION_EXCEPTION;
        case jogasaki::error_code::secondary_index_corruption_exception: return sql::error::SECONDARY_INDEX_CORRUPTION_EXCEPTION;
        case jogasaki::error_code::request_failure_exception: return sql::error::REQUEST_FAILURE_EXCEPTION;
        case jogasaki::error_code::transaction_not_found_exception: return sql::error::TRANSACTION_NOT_FOUND_EXCEPTION;
        case jogasaki::error_code::statement_not_found_exception: return sql::error::STATEMENT_NOT_FOUND_EXCEPTION;
        case jogasaki::error_code::internal_exception: return sql::error::INTERNAL_EXCEPTION;
        case jogasaki::error_code::unsupported_runtime_feature_exception: return sql::error::UNSUPPORTED_RUNTIME_FEATURE_EXCEPTION;
        case jogasaki::error_code::blocked_by_high_priority_transaction_exception: return sql::error::BLOCKED_BY_HIGH_PRIORITY_TRANSACTION_EXCEPTION;

        case jogasaki::error_code::compile_exception: return sql::error::COMPILE_EXCEPTION;
        case jogasaki::error_code::syntax_exception: return sql::error::SYNTAX_EXCEPTION;
        case jogasaki::error_code::analyze_exception: return sql::error::ANALYZE_EXCEPTION;
        case jogasaki::error_code::type_analyze_exception: return sql::error::TYPE_ANALYZE_EXCEPTION;
        case jogasaki::error_code::symbol_analyze_exception: return sql::error::SYMBOL_ANALYZE_EXCEPTION;
        case jogasaki::error_code::value_analyze_exception: return sql::error::VALUE_ANALYZE_EXCEPTION;
        case jogasaki::error_code::unsupported_compiler_feature_exception: return sql::error::UNSUPPORTED_COMPILER_FEATURE_EXCEPTION;

        case jogasaki::error_code::cc_exception: return sql::error::CC_EXCEPTION;
        case jogasaki::error_code::occ_exception: return sql::error::OCC_EXCEPTION;
        case jogasaki::error_code::occ_read_exception: return sql::error::OCC_READ_EXCEPTION;
        case jogasaki::error_code::conflict_on_write_preserve_exception: return sql::error::CONFLICT_ON_WRITE_PRESERVE_EXCEPTION;
        case jogasaki::error_code::occ_write_exception: return sql::error::OCC_WRITE_EXCEPTION;
        case jogasaki::error_code::ltx_exception: return sql::error::LTX_EXCEPTION;
        case jogasaki::error_code::ltx_read_exception: return sql::error::LTX_READ_EXCEPTION;
        case jogasaki::error_code::ltx_write_exception: return sql::error::LTX_WRITE_EXCEPTION;
        case jogasaki::error_code::rtx_exception: return sql::error::RTX_EXCEPTION;
        case jogasaki::error_code::blocked_by_concurrent_operation_exception: return sql::error::BLOCKED_BY_CONCURRENT_OPERATION_EXCEPTION;
    }
    std::abort();
}

inline jogasaki::error_code map_error(sql::error::Code s) {
    switch (s) {
        case sql::error::SQL_SERVICE_EXCEPTION: return jogasaki::error_code::sql_service_exception;
        case sql::error::SQL_EXECUTION_EXCEPTION: return jogasaki::error_code::sql_execution_exception;
        case sql::error::CONSTRAINT_VIOLATION_EXCEPTION: return jogasaki::error_code::constraint_violation_exception;
        case sql::error::UNIQUE_CONSTRAINT_VIOLATION_EXCEPTION: return jogasaki::error_code::unique_constraint_violation_exception;
        case sql::error::NOT_NULL_CONSTRAINT_VIOLATION_EXCEPTION: return jogasaki::error_code::not_null_constraint_violation_exception;
        case sql::error::REFERENTIAL_INTEGRITY_CONSTRAINT_VIOLATION_EXCEPTION: return jogasaki::error_code::referential_integrity_constraint_violation_exception;
        case sql::error::CHECK_CONSTRAINT_VIOLATION_EXCEPTION: return jogasaki::error_code::check_constraint_violation_exception;
        case sql::error::EVALUATION_EXCEPTION: return jogasaki::error_code::evaluation_exception;
        case sql::error::VALUE_EVALUATION_EXCEPTION: return jogasaki::error_code::value_evaluation_exception;
        case sql::error::SCALAR_SUBQUERY_EVALUATION_EXCEPTION: return jogasaki::error_code::scalar_subquery_evaluation_exception;
        case sql::error::TARGET_NOT_FOUND_EXCEPTION: return jogasaki::error_code::target_not_found_exception;
        case sql::error::TARGET_ALREADY_EXISTS_EXCEPTION: return jogasaki::error_code::target_already_exists_exception;
        case sql::error::INCONSISTENT_STATEMENT_EXCEPTION: return jogasaki::error_code::inconsistent_statement_exception;
        case sql::error::RESTRICTED_OPERATION_EXCEPTION: return jogasaki::error_code::restricted_operation_exception;
        case sql::error::DEPENDENCIES_VIOLATION_EXCEPTION: return jogasaki::error_code::dependencies_violation_exception;
        case sql::error::WRITE_OPERATION_BY_RTX_EXCEPTION: return jogasaki::error_code::write_operation_by_rtx_exception;
        case sql::error::LTX_WRITE_OPERATION_WITHOUT_WRITE_PRESERVE_EXCEPTION: return jogasaki::error_code::ltx_write_operation_without_write_preserve_exception;
        case sql::error::READ_OPERATION_ON_RESTRICTED_READ_AREA_EXCEPTION: return jogasaki::error_code::read_operation_on_restricted_read_area_exception;
        case sql::error::INACTIVE_TRANSACTION_EXCEPTION: return jogasaki::error_code::inactive_transaction_exception;
        case sql::error::PARAMETER_EXCEPTION: return jogasaki::error_code::parameter_exception;
        case sql::error::UNRESOLVED_PLACEHOLDER_EXCEPTION: return jogasaki::error_code::unresolved_placeholder_exception;
        case sql::error::LOAD_FILE_IOEXCEPTION: return jogasaki::error_code::load_file_ioexception;
        case sql::error::LOAD_FILE_NOT_FOUND_EXCEPTION: return jogasaki::error_code::load_file_not_found_exception;
        case sql::error::LOAD_FILE_FORMAT_EXCEPTION: return jogasaki::error_code::load_file_format_exception;
        case sql::error::DUMP_FILE_IOEXCEPTION: return jogasaki::error_code::dump_file_ioexception;
        case sql::error::DUMP_DIRECTORY_INACCESSIBLE_EXCEPTION: return jogasaki::error_code::dump_directory_inaccessible_exception;
        case sql::error::SQL_LIMIT_REACHED_EXCEPTION: return jogasaki::error_code::sql_limit_reached_exception;
        case sql::error::TRANSACTION_EXCEEDED_LIMIT_EXCEPTION: return jogasaki::error_code::transaction_exceeded_limit_exception;
        case sql::error::SQL_REQUEST_TIMED_OUT_EXCEPTION: return jogasaki::error_code::sql_request_timed_out_exception;
        case sql::error::DATA_CORRUPTION_EXCEPTION: return jogasaki::error_code::data_corruption_exception;
        case sql::error::SECONDARY_INDEX_CORRUPTION_EXCEPTION: return jogasaki::error_code::secondary_index_corruption_exception;
        case sql::error::REQUEST_FAILURE_EXCEPTION: return jogasaki::error_code::request_failure_exception;
        case sql::error::TRANSACTION_NOT_FOUND_EXCEPTION: return jogasaki::error_code::transaction_not_found_exception;
        case sql::error::STATEMENT_NOT_FOUND_EXCEPTION: return jogasaki::error_code::statement_not_found_exception;
        case sql::error::INTERNAL_EXCEPTION: return jogasaki::error_code::internal_exception;
        case sql::error::UNSUPPORTED_RUNTIME_FEATURE_EXCEPTION: return jogasaki::error_code::unsupported_runtime_feature_exception;
        case sql::error::BLOCKED_BY_HIGH_PRIORITY_TRANSACTION_EXCEPTION: return jogasaki::error_code::blocked_by_high_priority_transaction_exception;

        case sql::error::COMPILE_EXCEPTION: return jogasaki::error_code::compile_exception;
        case sql::error::SYNTAX_EXCEPTION: return jogasaki::error_code::syntax_exception;
        case sql::error::ANALYZE_EXCEPTION: return jogasaki::error_code::analyze_exception;
        case sql::error::TYPE_ANALYZE_EXCEPTION: return jogasaki::error_code::type_analyze_exception;
        case sql::error::SYMBOL_ANALYZE_EXCEPTION: return jogasaki::error_code::symbol_analyze_exception;
        case sql::error::VALUE_ANALYZE_EXCEPTION: return jogasaki::error_code::value_analyze_exception;
        case sql::error::UNSUPPORTED_COMPILER_FEATURE_EXCEPTION: return jogasaki::error_code::unsupported_compiler_feature_exception;

        case sql::error::CC_EXCEPTION: return jogasaki::error_code::cc_exception;
        case sql::error::OCC_EXCEPTION: return jogasaki::error_code::occ_exception;
        case sql::error::OCC_READ_EXCEPTION: return jogasaki::error_code::occ_read_exception;
        case sql::error::CONFLICT_ON_WRITE_PRESERVE_EXCEPTION: return jogasaki::error_code::conflict_on_write_preserve_exception;
        case sql::error::OCC_WRITE_EXCEPTION: return jogasaki::error_code::occ_write_exception;
        case sql::error::LTX_EXCEPTION: return jogasaki::error_code::ltx_exception;
        case sql::error::LTX_READ_EXCEPTION: return jogasaki::error_code::ltx_read_exception;
        case sql::error::LTX_WRITE_EXCEPTION: return jogasaki::error_code::ltx_write_exception;
        case sql::error::RTX_EXCEPTION: return jogasaki::error_code::rtx_exception;
        case sql::error::BLOCKED_BY_CONCURRENT_OPERATION_EXCEPTION: return jogasaki::error_code::blocked_by_concurrent_operation_exception;

        default: return jogasaki::error_code::sql_service_exception;
    }
    std::abort();
}
}

