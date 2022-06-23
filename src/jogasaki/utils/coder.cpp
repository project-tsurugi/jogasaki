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
#include "coder.h"

#include <takatori/util/fail.h>

#include <jogasaki/kvs/writable_stream.h>
#include <jogasaki/executor/process/impl/expression/any.h>

namespace jogasaki::utils {

using takatori::util::fail;

status encode_any(
    data::aligned_buffer& target,
    meta::field_type const& type,
    bool nullable,
    kvs::coding_spec spec,
    std::initializer_list<executor::process::impl::expression::any> sources
) {
    std::size_t length = 0;
    for(int loop = 0; loop < 2; ++loop) { // if first trial overflows `buf`, extend it and retry
        kvs::writable_stream s{target.data(), target.capacity(), loop == 0};
        for(auto&& f : sources) {
            if (f.empty()) {
                // value not specified for the field
                if (! nullable) {
                    fail();
                }
                if(auto res = kvs::encode_nullable(f, type, spec, s); res != status::ok) {
                    return res;
                }
            } else {
                if (nullable) {
                    if(auto res = kvs::encode_nullable(f, type, spec, s); res != status::ok) {
                        return res;
                    }
                } else {
                    if(auto res = kvs::encode(f, type, spec, s); res != status::ok) {
                        return res;
                    }
                }
            }
        }
        length = s.size();
        bool fit = length <= target.capacity();
        target.resize(length);
        if (loop == 0) {
            if (fit) {
                break;
            }
            target.resize(0); // set data size 0 and start from beginning
        }
    }
    return status::ok;
}

}

