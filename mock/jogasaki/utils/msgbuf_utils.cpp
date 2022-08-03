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
#include "msgbuf_utils.h"

#include <sstream>

#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/serializer/value_input.h>

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
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;

using takatori::util::maybe_shared_ptr;

void set_null(accessor::record_ref ref, std::size_t index, meta::record_meta& meta) {
    ref.set_null(meta.nullity_offset(index), true);
}

std::vector<mock::basic_record> deserialize_msg(
    std::string_view data,
    meta::record_meta& meta,
    deserialize_result& result
) {
    takatori::util::buffer_view buf{const_cast<char*>(data.data()), data.size()};
    std::vector<mock::basic_record> ret{};
    auto it = buf.cbegin();
    auto end = buf.cend();
    while(it < end) {
        auto typ = takatori::serializer::peek_type(it, end);
        if (typ == takatori::serializer::entry_type::row) {
            auto num_columns = takatori::serializer::read_row_begin(it, end);
            BOOST_ASSERT(num_columns == meta.field_count());
            continue;
        }
        if (typ == takatori::serializer::entry_type::end_of_contents) {
            takatori::serializer::read_end_of_contents(it, end);
            BOOST_ASSERT(it == end);
            result.saw_end_of_contents_ = true;
            break;
        }
        auto& record = ret.emplace_back(maybe_shared_ptr{&meta});
        auto ref = record.ref();
        for (std::size_t index = 0, n = meta.field_count(); index < n ; index++) {
            if(auto tp = takatori::serializer::peek_type(it, end); tp == takatori::serializer::entry_type::null) {
                set_null(ref, index, meta);
                continue;
            }
            switch (meta.at(index).kind()) {
                case jogasaki::meta::field_type_kind::int4: ref.set_value<std::int32_t>(meta.value_offset(index), static_cast<std::int32_t>(takatori::serializer::read_int(it, end))); break;
                case jogasaki::meta::field_type_kind::int8: ref.set_value<std::int64_t>(meta.value_offset(index), takatori::serializer::read_int(it, end)); break;
                case jogasaki::meta::field_type_kind::float4: ref.set_value<float>(meta.value_offset(index), takatori::serializer::read_float4(it, end)); break;
                case jogasaki::meta::field_type_kind::float8: ref.set_value<double>(meta.value_offset(index), takatori::serializer::read_float8(it, end)); break;
                case jogasaki::meta::field_type_kind::character: {
                    auto v = takatori::serializer::read_character(it, end);
                    auto sv = record.allocate_varlen_data(v);
                    record.ref().set_value(meta.value_offset(index), accessor::text{sv});
                    break;
                }
                default:
                    std::abort();
            }
        }
    }
    return ret;
}

std::vector<mock::basic_record> deserialize_msg(
    std::vector<std::string_view> const& data,
    meta::record_meta& meta,
    deserialize_result& result
) {
    std::vector<mock::basic_record> ret{};
    for(auto sv : data) {
        auto res = deserialize_msg(sv, meta, result);
        ret.insert(ret.end(), std::move_iterator{res.begin()}, std::move_iterator{res.end()});
    }
    return ret;
}

std::vector<mock::basic_record> deserialize_msg(std::string_view data, meta::record_meta& meta) {
    deserialize_result result{};
    return deserialize_msg(data, meta, result);
}

std::vector<mock::basic_record> deserialize_msg(std::vector<std::string_view> const& data, meta::record_meta& meta) {
    deserialize_result result{};
    return deserialize_msg(data, meta, result);
}
}
