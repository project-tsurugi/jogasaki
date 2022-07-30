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
#include "result_serialization.h"

#include <msgpack.hpp>

#include <takatori/util/fail.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/meta/record_meta.h>

namespace jogasaki::utils {

using takatori::util::fail;

bool write_msg(accessor::record_ref rec, msgpack::sbuffer& buf, meta::record_meta* meta) {
    if(buf.size() == 0) {
        buf = msgpack::sbuffer{writer_work_buffer_size}; // automatically expands when capacity is not sufficient
    }
    buf.clear();
    for (std::size_t i=0, n=meta->field_count(); i < n; ++i) {
        if (rec.is_null(meta->nullity_offset(i))) {
            msgpack::pack(buf, msgpack::type::nil_t());
        } else {
            using k = jogasaki::meta::field_type_kind;
            auto os = meta->value_offset(i);
            switch (meta->at(i).kind()) {
                case k::int4: msgpack::pack(buf, rec.get_value<meta::field_type_traits<k::int4>::runtime_type>(os)); break;
                case k::int8: msgpack::pack(buf, rec.get_value<meta::field_type_traits<k::int8>::runtime_type>(os)); break;
                case k::float4: msgpack::pack(buf, rec.get_value<meta::field_type_traits<k::float4>::runtime_type>(os)); break;
                case k::float8: msgpack::pack(buf, rec.get_value<meta::field_type_traits<k::float8>::runtime_type>(os)); break;
                case k::character: {
                    auto text = rec.get_value<meta::field_type_traits<k::character>::runtime_type>(os);
                    msgpack::pack(buf, static_cast<std::string_view>(text));
                    break;
                }
                default:
                    // FIXME decimal, temp. types
                    fail();
            }
        }
    }
    return true;
}
}

