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
#include <jogasaki/executor/process/impl/expression/error.h>
#include <jogasaki/executor/process/impl/expression/evaluator.h>
#include <jogasaki/executor/process/impl/expression/evaluator_context.h>
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

namespace jogasaki::executor::process::impl::ops::details {

status encode_key(  //NOLINT(readability-function-cognitive-complexity)
    std::vector<details::search_key_field_info> const& keys,
    variable_table& input_variables,
    memory::lifo_paged_memory_resource& resource,
    data::aligned_buffer& out,
    std::size_t& length
) {
    utils::checkpoint_holder cph(std::addressof(resource));
    length = 0;
    for(int loop = 0; loop < 2; ++loop) { // if first trial overflows `buf`, extend it and retry
        kvs::writable_stream s{out.data(), out.capacity(), loop == 0};
        for(auto&& k : keys) {
            expression::evaluator_context ctx{std::addressof(resource)};
            auto a = k.evaluator_(ctx, input_variables, &resource);
            if (a.error()) {
                VLOG_LP(log_error) << "evaluation error: " << a.to<expression::error>();
                return status::err_expression_evaluation_failure;
            }
            if(! utils::convert_any(a, k.type_)) {
                VLOG_LP(log_error) << "type mismatch: expected " << k.type_ << ", value index is " << a.type_index();
                return status::err_expression_evaluation_failure;
            }
            if (k.nullable_) {
                if(auto res = kvs::encode_nullable(a, k.type_, k.spec_, s);res != status::ok) {
                    return res;
                }
            } else {
                if(a.empty()) {
                    VLOG_LP(log_error) << "Null assigned for non-nullable field.";
                    return status::err_integrity_constraint_violation;
                }
                if(auto res = kvs::encode(a, k.type_, k.spec_, s);res != status::ok) {
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
