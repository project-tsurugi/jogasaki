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
#include "encode_key.h"

#include <ostream>
#include <utility>
#include <glog/logging.h>

#include <jogasaki/data/any.h>
#include <jogasaki/executor/expr/error.h>
#include <jogasaki/executor/expr/evaluator.h>
#include <jogasaki/executor/expr/evaluator_context.h>
#include <jogasaki/executor/process/impl/ops/details/search_key_field_info.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/kvs/writable_stream.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/status.h>
#include <jogasaki/utils/checkpoint_holder.h>
#include <jogasaki/utils/convert_any.h>
#include <jogasaki/utils/handle_kvs_errors.h>
#include <jogasaki/utils/make_function_context.h>
namespace jogasaki::executor::process::impl::ops::details {

status encode_key(  //NOLINT(readability-function-cognitive-complexity)
    request_context* context,
    std::vector<details::search_key_field_info> const& keys,
    variable_table& input_variables,
    memory::lifo_paged_memory_resource& resource,
    data::aligned_buffer& out,
    std::size_t& length,
    std::string& message
) {
    utils::checkpoint_holder cph(std::addressof(resource));
    length = 0;
    for(int loop = 0; loop < 2; ++loop) { // if first trial overflows `buf`, extend it and retry
        kvs::writable_stream s{out.data(), out.capacity(), loop == 0};
        for(auto&& k : keys) {
            expr::evaluator_context ctx{
                std::addressof(resource),
                context ? utils::make_function_context(*context->transaction()) : nullptr,
            };
            auto a = k.evaluator_(ctx, input_variables, &resource);
            if (a.error()) {
                VLOG_LP(log_error) << "evaluation error: " << a.to<expr::error>();
                return status::err_expression_evaluation_failure;
            }
            if(! utils::convert_any(a, k.type_)) {
                std::stringstream ss{};
                ss << "unsupported type conversion to:" << k.type_ << " from:" << type_name(a);
                VLOG_LP(log_error) << ss.str();
                message = ss.str();
                return status::err_type_mismatch;
            }
            kvs::coding_context cctx{};
            if (k.nullable_) {
                if(auto res = kvs::encode_nullable(a, k.type_, k.spec_, cctx, s);res != status::ok) {
                    return res;
                }
            } else {
                if(a.empty()) {
                    // log level was lowered temporarily to address issue #939
                    VLOG_LP(log_debug) << "Null assigned for non-nullable field.";
                    return status::err_integrity_constraint_violation;
                }
                if(auto res = kvs::encode(a, k.type_, k.spec_, cctx, s);res != status::ok) {
                    return res;
                }
            }
            cph.reset();
        }
        length = s.size();
        bool fit = length <= out.capacity();
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

status two_encode_keys(
    request_context* context,
    std::vector<details::search_key_field_info> const& begin_keys,
    std::vector<details::search_key_field_info> const& end_keys,
    variable_table& input_variables,
    memory::lifo_paged_memory_resource& resource,
    data::aligned_buffer& key_begin,
    std::size_t& blen,
    data::aligned_buffer& key_end,
    std::size_t& elen
) {
    status status_result = status::ok;
    std::string message;
    if ((status_result = impl::ops::details::encode_key(
            context, begin_keys, input_variables, resource, key_begin, blen, message)) 
        != status::ok) {
        
        if (status_result == status::err_type_mismatch) {
            set_error(*context, error_code::unsupported_runtime_feature_exception, message, status_result);
        }
        return status_result;
    }
    if ((status_result = impl::ops::details::encode_key(
            context, end_keys, input_variables, resource, key_end, elen, message)) 
        != status::ok) {
        
        if (status_result == status::err_type_mismatch) {
            set_error(*context, error_code::unsupported_runtime_feature_exception, message, status_result);
        }
        return status_result;
    }
    return status::ok;
}

}  // namespace jogasaki::executor::process::impl::ops::details