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
#include "encode_key.h"

#include <jogasaki/logging.h>
#include <jogasaki/kvs/writable_stream.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/utils/checkpoint_holder.h>
#include <jogasaki/utils/convert_any.h>

namespace jogasaki::executor::process::impl::ops::details {

status encode_key(
    std::vector<details::search_key_field_info> const& keys,
    variable_table& input_variables,
    memory::lifo_paged_memory_resource& resource,
    data::aligned_buffer& out,
    std::size_t& length
) {
    utils::checkpoint_holder cph(std::addressof(resource));
    length = 0;
    for(int loop = 0; loop < 2; ++loop) { // first calculate buffer length, and then allocate/fill
        kvs::writable_stream s{out.data(), loop == 0 ? 0 : out.size()};
        for(auto&& k : keys) {
            auto a = k.evaluator_(input_variables, &resource);
            if (a.error()) {
                VLOG(log_error) << "evaluation error: " << a.to<expression::error>();
                return status::err_expression_evaluation_failure;
            }
            if(! utils::convert_any(a, k.type_)) {
                VLOG(log_error) << "type mismatch: expected " << k.type_ << ", value index is " << a.type_index();
                return status::err_expression_evaluation_failure;
            }
            if (k.nullable_) {
                kvs::encode_nullable(a, k.type_, k.spec_, s);
            } else {
                if(a.empty()) {
                    VLOG(log_error) << "Null assigned for non-nullable field.";
                    return status::err_integrity_constraint_violation;
                }
                kvs::encode(a, k.type_, k.spec_, s);
            }
            cph.reset();
        }
        length = s.size();
        if (loop == 0 && out.size() < length) {
            out.resize(length);
        }
    }
    return status::ok;
}

}

