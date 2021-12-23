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
#pragma once

#include <sstream>

#include <msgpack.hpp>
#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/mock/basic_record.h>
#include <jogasaki/utils/storage_data.h>
#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/impl/record.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/executor/sequence/sequence.h>
#include <jogasaki/executor/sequence/manager.h>
#include <jogasaki/utils/binary_printer.h>

namespace jogasaki::utils {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;

using takatori::util::maybe_shared_ptr;

template <typename T>
static inline bool extract(std::string_view data, T &v, std::size_t &offset) {
    // msgpack::unpack() may throw msgpack::unpack_error with "parse error" or "insufficient bytes" message.
    msgpack::unpacked result = msgpack::unpack(data.data(), data.size(), offset);
    const msgpack::object obj(result.get());
    if (obj.type == msgpack::type::NIL) {
        return false;
    }
    v = obj.as<T>();
    return true;
}

inline void set_null(accessor::record_ref ref, std::size_t index, meta::record_meta& meta) {
    ref.set_null(meta.nullity_offset(index), true);
}

template <typename T>
static inline void set_value(
    std::string_view data,
    std::size_t &offset,
    accessor::record_ref ref,
    std::size_t index,
    meta::record_meta& meta
) {
    T v;
    if (extract(data, v, offset)) {
        ref.set_value(meta.value_offset(index), v);
    } else {
        set_null(ref, index, meta);
    }
}

inline std::vector<mock::basic_record> deserialize_msg(std::string_view data, jogasaki::meta::record_meta& meta) {
    std::vector<mock::basic_record> ret{};
    std::size_t offset{};
    while(offset < data.size()) {
        auto& record = ret.emplace_back(maybe_shared_ptr{&meta});
        auto ref = record.ref();
        for (std::size_t index = 0, n = meta.field_count(); index < n ; index++) {
            switch (meta.at(index).kind()) {
                case jogasaki::meta::field_type_kind::int4: set_value<std::int32_t>(data, offset, ref, index, meta); break;
                case jogasaki::meta::field_type_kind::int8: set_value<std::int64_t>(data, offset, ref, index, meta); break;
                case jogasaki::meta::field_type_kind::float4: set_value<float>(data, offset, ref, index, meta); break;
                case jogasaki::meta::field_type_kind::float8: set_value<double>(data, offset, ref, index, meta); break;
                case jogasaki::meta::field_type_kind::character: {
                    std::string v{};
                    if (extract(data, v, offset)) {
                        auto sv = record.allocate_varlen_data(v);
                        record.ref().set_value(meta.value_offset(index), accessor::text{sv});
                    } else {
                        set_null(record.ref(), index, meta);
                    }
                    break;
                }
                default:
                    std::abort();
            }
        }
    }
    return ret;
}

}
