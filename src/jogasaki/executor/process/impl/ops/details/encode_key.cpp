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

#include <takatori/util/fail.h>

#include <jogasaki/kvs/writable_stream.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/utils/checkpoint_holder.h>

namespace jogasaki::executor::process::impl::ops::details {

using takatori::util::fail;

std::size_t encode_key(
    std::vector<details::search_key_field_info> const& keys,
    variable_table& input_variables,
    memory::lifo_paged_memory_resource& resource,
    data::aligned_buffer& out
) {
    utils::checkpoint_holder cph(std::addressof(resource));
    std::size_t len = 0;
    for(int loop = 0; loop < 2; ++loop) { // first calculate buffer length, and then allocate/fill
        kvs::writable_stream s{out.data(), loop == 0 ? 0 : out.size()};
        for(auto&& k : keys) {
            auto a = k.evaluator_(input_variables, &resource);
            if (a.error()) {
                LOG(ERROR) << "evaluation error: " << a.to<expression::error>();
                fail();
            }
            if (k.nullable_) {
                kvs::encode_nullable(a, k.type_, k.spec_, s);
            } else {
                BOOST_ASSERT(! a.empty());  //NOLINT
                kvs::encode(a, k.type_, k.spec_, s);
            }
            cph.reset();
        }
        len = s.size();
        if (loop == 0 && out.size() < len) {
            out.resize(len);
        }
    }
    return len;
}

}

