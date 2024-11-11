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
            if(a.empty()) {
                // creating search key with null value makes no sense because it does not match any entry
                return status::err_integrity_constraint_violation;
            }
            kvs::coding_context cctx{};
            if (k.nullable_) {
                if(auto res = kvs::encode_nullable(a, k.type_, k.spec_, cctx, s);res != status::ok) {
                    return res;
                }
            } else {
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

}  // namespace jogasaki::executor::process::impl::ops::details
