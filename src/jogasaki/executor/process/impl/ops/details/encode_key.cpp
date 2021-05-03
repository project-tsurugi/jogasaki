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

#include <jogasaki/kvs/writable_stream.h>
#include <jogasaki/kvs/coder.h>

namespace jogasaki::executor::process::impl::ops::details {

namespace relation = takatori::relation;

void encode_key(
    std::vector<details::search_key_field_info> const& keys,
    variable_table& vars,
    memory::lifo_paged_memory_resource& resource,
    data::aligned_buffer& out
) {
    auto cp = resource.get_checkpoint();
    for(int loop = 0; loop < 2; ++loop) { // first calculate buffer length, and then allocate/fill
        kvs::writable_stream s{out.data(), loop == 0 ? 0 : out.size()};
        std::size_t i = 0;
        for(auto&& k : keys) {
            auto res = k.evaluator_(vars, &resource);
            kvs::encode(res, k.type_, k.spec_, s);
            resource.deallocate_after(cp);
            ++i;
        }
        if (loop == 0) {
            out.resize(s.size());
        }
    }
}

}

