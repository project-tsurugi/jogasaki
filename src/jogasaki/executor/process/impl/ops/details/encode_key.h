/*
 * Copyright 2018-2024 Project Tsurugi.
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
#include <vector>

#include <jogasaki/data/aligned_buffer.h>
#include <jogasaki/executor/expr/evaluator_context.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/request_context.h>
#include <jogasaki/status.h>

#include "search_key_field_info.h"

namespace jogasaki::executor::process::impl::ops::details {

/**
 * @brief evaluate the search key and encode
 * @details evaluate the search key fields and encode them into the output buffer for a point lookup.
 * @param ectx the evaluator context
 * @param keys the key fields to be evaluated
 * @param input_variables the variables to be used for evaluation
 * @param resource the memory resource
 * @param out the buffer to store the encoded key
 * @param length the length of the encoded key
 * @param context request context for error reporting (may be nullptr),
 * the error info. is filled only when err_type_mismatch is returned
 * @return status::ok when successful
 * @return status::err_integrity_constraint_violation when evaluation results contain null, with that search
 * should not find any entry (this is not necessarily integrity constraint violation, but the status code is left
 * unchanged for backward compatibility)
 * @return status::err_type_mismatch if the type of the evaluated value does not match the expected type
 * `context` (if available) is filled with error info. when this erorr is returned.
 * @return status::err_expression_evaluation_failure any other evaluation failure
 */
status encode_key(
    expr::evaluator_context& ectx,
    std::vector<details::search_key_field_info> const& keys,
    executor::process::impl::variable_table& input_variables,
    memory::lifo_paged_memory_resource& resource,
    data::aligned_buffer& out,
    std::size_t& length,
    request_context* context
);
/**
 * @brief encode begin and end keys for a scan, handling primary vs secondary index differences.
 * @details Encodes logical lower/upper scan bounds into physical begin/end keys.
 *
 * For primary index scans (`use_secondary = false`):
 * - lower_fields are encoded directly into key_begin; upper_fields into key_end.
 * - begin_kind_out and end_kind_out are set to lower_kind and upper_kind respectively.
 * - No DESC swap or null-exclusion logic is applied.
 *
 * For secondary index scans (`use_secondary = true`), the following are handled:
 * - DESC trailing column: maps upper bound to physical begin and lower bound to physical end.
 * - Nullable unbound trailing column: appends the non-null indicator byte to exclude null
 *   entries, and sets the endpoint kind to prefixed_inclusive.
 * - Full-key endpoint (n_total == n_total_secondary_cols): converts the endpoint kind to its
 *   prefixed variant to prevent the stored PK suffix from affecting boundary comparison.
 *
 * @param ectx the evaluator context
 * @param context request context for error reporting (may be nullptr),
 * the error info. is filled only when err_type_mismatch is returned
 * @param lower_fields key fields for the lower scan bound (logical minimum side)
 * @param lower_kind endpoint kind for the lower bound
 * @param upper_fields key fields for the upper scan bound (logical maximum side)
 * @param upper_kind endpoint kind for the upper bound
 * @param input_variables the variables to be used for evaluation
 * @param resource the memory resource
 * @param key_begin the buffer to store the physical begin encoded key
 * @param blen the length of the begin encoded key
 * @param begin_kind_out the endpoint kind for the physical begin key
 * @param key_end the buffer to store the physical end encoded key
 * @param elen the length of the end encoded key
 * @param end_kind_out the endpoint kind for the physical end key
 * @return status::ok when successful
 * @return status::err_unsupported if lower_kind or upper_kind is inclusive or exclusive
 * @return status::err_integrity_constraint_violation when a key field evaluates to null (no match)
 * @return status::err_type_mismatch if the type of the evaluated value does not match the expected type
 * `context` (if available) is filled with error info. when this erorr is returned.
 * @return status::err_expression_evaluation_failure any other evaluation failure
 */
status encode_scan_keys(
    expr::evaluator_context& ectx,
    request_context* context,
    std::vector<details::search_key_field_info> const& lower_fields,
    kvs::end_point_kind lower_kind,
    std::vector<details::search_key_field_info> const& upper_fields,
    kvs::end_point_kind upper_kind,
    variable_table& input_variables,
    memory::lifo_paged_memory_resource& resource,
    data::aligned_buffer& key_begin,
    std::size_t& blen,
    kvs::end_point_kind& begin_kind_out,
    data::aligned_buffer& key_end,
    std::size_t& elen,
    kvs::end_point_kind& end_kind_out
);

}  // namespace jogasaki::executor::process::impl::ops::details
