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
#include "encode_key.h"

#include <cstdint>
#include <ostream>
#include <sstream>
#include <utility>
#include <glog/logging.h>

#include <jogasaki/data/any.h>
#include <jogasaki/executor/expr/error.h>
#include <jogasaki/executor/expr/evaluator.h>
#include <jogasaki/executor/expr/evaluator_context.h>
#include <jogasaki/executor/process/impl/ops/details/search_key_field_info.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/kvs/writable_stream.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/status.h>
#include <jogasaki/utils/assert.h>
#include <jogasaki/utils/checkpoint_holder.h>
#include <jogasaki/utils/convert_any.h>
#include <jogasaki/utils/handle_kvs_errors.h>

namespace jogasaki::executor::process::impl::ops::details {

namespace {

/**
 * @brief encode one side (begin or end) of the secondary index scan key into a buffer
 * @param ectx evaluator context
 * @param context request context for error reporting (may be nullptr),
 * the error info. is filled only when err_type_mismatch is returned
 * @param fields fields to encode (the selected lower or upper fields after DESC swap)
 * @param n_total total number of secondary index key columns
 * @param side_unbound true when the trailing column is absent from fields (unbound)
 * @param trailing_nullable true when the trailing column is nullable
 * @param trailing_spec coding spec (direction) of the trailing column
 * @param input_variables variable table for evaluation
 * @param resource memory resource
 * @param out output buffer
 * @param length output: encoded byte length
 * @return status::ok on success, error code otherwise
 * @return status::err_type_mismatch if the type of the evaluated value does not match the expected type
 * `context` (if available) is filled with error info. when this erorr is returned.
 */
status encode_scan_side(  //NOLINT(readability-function-cognitive-complexity)
    expr::evaluator_context& ectx,
    request_context* context,
    std::vector<search_key_field_info> const& fields,
    bool side_unbound,
    bool trailing_nullable,
    kvs::coding_spec trailing_spec,
    variable_table& input_variables,
    memory::lifo_paged_memory_resource& resource,
    data::aligned_buffer& out,
    std::size_t& length
) {
    utils::checkpoint_holder cph(std::addressof(resource));
    length = 0;
    for(int loop = 0; loop < 2; ++loop) { // if first trial overflows `out`, extend it and retry
        kvs::writable_stream s{out.data(), out.capacity(), loop == 0};
        for (auto const& field : fields) {
            auto a = field.evaluator_(ectx, input_variables, &resource);
            if (a.error()) {
                VLOG_LP(log_error) << "evaluation error: " << a.to<expr::error>();
                return status::err_expression_evaluation_failure;
            }
            if (! utils::convert_any(a, field.type_)) {
                std::stringstream ss{};
                ss << "unsupported type conversion to:" << field.type_ << " from:" << type_name(a);
                VLOG_LP(log_error) << ss.str();
                if (context != nullptr) {
                    set_error_context(*context, error_code::unsupported_runtime_feature_exception,
                        ss.str(), status::err_type_mismatch);
                }
                return status::err_type_mismatch;
            }
            if (a.empty()) {
                // creating search key with null value makes no sense because it does not match any entry
                return status::err_integrity_constraint_violation;
            }
            kvs::coding_context cctx{};
            status res{};
            if (field.nullable_) {
                res = kvs::encode_nullable(a, field.type_, field.spec_, cctx, s);
            } else {
                res = kvs::encode(a, field.type_, field.spec_, cctx, s);
            }
            if (res != status::ok) {
                return res;
            }
            cph.reset();
        }
        if (side_unbound && trailing_nullable) {
            // Append the non-null indicator byte to exclude null entries from the scan.
            // For ASC columns, this writes 0x81 (non-null byte in ascending encoding).
            // For DESC columns, this writes 0x7E (non-null byte in descending encoding).
            if (auto res = s.write<std::int8_t>(1, trailing_spec.ordering()); res != status::ok) {
                return res;
            }
        }
        length = s.size();
        bool const fit = (length <= out.capacity());
        out.resize(length);
        if (loop == 0) {
            if (fit) {
                break;
            }
            out.resize(0); // set data size 0 and start from beginning
        }
    }
    return status::ok;
}

}  // namespace

status encode_key(
    expr::evaluator_context& ectx,
    std::vector<details::search_key_field_info> const& keys,
    variable_table& input_variables,
    memory::lifo_paged_memory_resource& resource,
    data::aligned_buffer& out,
    std::size_t& length,
    request_context* context
) {
    kvs::coding_spec unused{};
    return encode_scan_side(ectx, context, keys, false, false, unused,
        input_variables, resource, out, length);
}

status encode_scan_keys(
    expr::evaluator_context& ectx,
    request_context* context,
    std::vector<search_key_field_info> const& lower_fields,
    kvs::end_point_kind lower_kind,
    std::vector<search_key_field_info> const& upper_fields,
    kvs::end_point_kind upper_kind,
    variable_table& input_variables,
    memory::lifo_paged_memory_resource& resource,
    data::aligned_buffer& key_begin,
    std::size_t& blen,
    kvs::end_point_kind& begin_kind_out,
    data::aligned_buffer& key_end,
    std::size_t& elen,
    kvs::end_point_kind& end_kind_out
) {
    auto const n_lower = lower_fields.size();
    auto const n_upper = upper_fields.size();

    // full scan: both endpoints unbound — no encoding needed
    if (n_lower == 0 && n_upper == 0) {
        blen = 0;
        elen = 0;
        key_begin.resize(0);
        key_end.resize(0);
        begin_kind_out = lower_kind;
        end_kind_out = upper_kind;
        return status::ok;
    }

    // Planner premise: lower and upper may differ in length by at most 1 (trailing column may be
    // present in only one endpoint when the other is unbound on that column).
    assert_with_exception(n_lower <= n_upper + 1 && n_upper <= n_lower + 1, n_lower, n_upper);

    auto const n_total = std::max(n_lower, n_upper);
    // Determine the trailing column's spec from whichever endpoint has it.
    search_key_field_info const& trailing_field =
        (n_lower >= n_upper) ? lower_fields[n_total - 1] : upper_fields[n_total - 1];
    bool const trailing_asc = (trailing_field.spec_.ordering() == kvs::order::ascending);
    bool const trailing_nullable = trailing_field.nullable_;

    // DESC swap: for a DESC trailing column the physical storage order is inverted relative to
    // logical order, so upper maps to physical begin and lower maps to physical end.
    // This applies regardless of which endpoint specifies the trailing column.
    bool const swap = ! trailing_asc;
    std::vector<search_key_field_info> const& begin_fields = swap ? upper_fields : lower_fields;
    std::vector<search_key_field_info> const& end_fields = swap ? lower_fields : upper_fields;
    auto const raw_begin_kind = swap ? upper_kind : lower_kind;
    auto const raw_end_kind   = swap ? lower_kind : upper_kind;

    bool const begin_unbound = (begin_fields.size() < n_total);
    bool const end_unbound   = (end_fields.size() < n_total);

    // Compute output endpoint kinds.
    // Nullable unbound trailing column: the trailing column is absent from the fields vector.
    // Append a non-null indicator byte (done in encode_scan_side) and force prefixed_inclusive
    // so the byte is treated as a prefix bound that excludes null entries.
    begin_kind_out = begin_unbound && trailing_nullable ? kvs::end_point_kind::prefixed_inclusive : raw_begin_kind;
    end_kind_out = end_unbound && trailing_nullable ? kvs::end_point_kind::prefixed_inclusive : raw_end_kind;

    if (auto res = encode_scan_side(ectx, context, begin_fields, begin_unbound,
            trailing_nullable, trailing_field.spec_, input_variables, resource,
            key_begin, blen); res != status::ok) {
        return res;
    }
    return encode_scan_side(ectx, context, end_fields, end_unbound,
        trailing_nullable, trailing_field.spec_, input_variables, resource,
        key_end, elen);
}

}  // namespace jogasaki::executor::process::impl::ops::details
