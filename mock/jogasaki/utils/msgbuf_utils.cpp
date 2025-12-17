/*
 * Copyright 2018-2025 Project Tsurugi.
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

#include <cstdlib>
#include <iterator>
#include <memory>
#include <cstdint>
#include <string>
#include <variant>
#include <boost/assert.hpp>

#include <takatori/util/buffer_view.h>
#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/accessor/text.h>
#include <jogasaki/executor/compare_info.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_traits.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/meta/time_of_day_field_option.h>
#include <jogasaki/meta/time_point_field_option.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/task.h>
#include <jogasaki/request_context.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/serializer/entry_type.h>
#include <jogasaki/serializer/value_input.h>

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
    meta::record_meta& meta
) {
    takatori::util::buffer_view buf{const_cast<char*>(data.data()), data.size()};
    std::vector<mock::basic_record> ret{};
    auto it = buf.cbegin();
    auto end = buf.cend();
    while(it < end) {
        auto typ = serializer::peek_type(it, end);
        if (typ == serializer::entry_type::row) {
            [[maybe_unused]] auto num_columns = serializer::read_row_begin(it, end);
            BOOST_ASSERT(num_columns == meta.field_count());
            continue;
        }
        auto& record = ret.emplace_back(maybe_shared_ptr{&meta});
        auto ref = record.ref();
        for (std::size_t index = 0, n = meta.field_count(); index < n ; index++) {
            if(auto tp = serializer::peek_type(it, end); tp == serializer::entry_type::null) {
                set_null(ref, index, meta);
                serializer::read_null(it, end);
                continue;
            }
            auto& fld = meta.at(index);
            switch (fld.kind()) {
                case jogasaki::meta::field_type_kind::boolean: ref.set_value<std::int8_t>(meta.value_offset(index), static_cast<std::int8_t>(serializer::read_int(it, end))); break;
                case jogasaki::meta::field_type_kind::int4: ref.set_value<std::int32_t>(meta.value_offset(index), static_cast<std::int32_t>(serializer::read_int(it, end))); break;
                case jogasaki::meta::field_type_kind::int8: ref.set_value<std::int64_t>(meta.value_offset(index), serializer::read_int(it, end)); break;
                case jogasaki::meta::field_type_kind::float4: ref.set_value<float>(meta.value_offset(index), serializer::read_float4(it, end)); break;
                case jogasaki::meta::field_type_kind::float8: ref.set_value<double>(meta.value_offset(index), serializer::read_float8(it, end)); break;
                case jogasaki::meta::field_type_kind::decimal: ref.set_value<runtime_t<meta::field_type_kind::decimal>>(meta.value_offset(index), serializer::read_decimal(it, end)); break;
                case jogasaki::meta::field_type_kind::character: {
                    auto v = serializer::read_character(it, end);
                    auto sv = record.allocate_varlen_data(v);
                    record.ref().set_value(meta.value_offset(index), accessor::text{sv});
                    break;
                }
                case jogasaki::meta::field_type_kind::octet: {
                    auto v = serializer::read_octet(it, end);
                    auto sv = record.allocate_varlen_data(v);
                    record.ref().set_value(meta.value_offset(index), accessor::binary{sv});
                    break;
                }
                case jogasaki::meta::field_type_kind::date: ref.set_value<runtime_t<meta::field_type_kind::date>>(meta.value_offset(index), serializer::read_date(it, end)); break;
                case jogasaki::meta::field_type_kind::time_of_day: {
                    if(fld.option_unsafe<jogasaki::meta::field_type_kind::time_of_day>()->with_offset_) {
                        auto [v, o] = serializer::read_time_of_day_with_offset(it, end);
                        (void) o; // currently ignoring offset
                        ref.set_value<runtime_t<meta::field_type_kind::time_of_day>>(meta.value_offset(index), v);
                        break;
                    }
                    ref.set_value<runtime_t<meta::field_type_kind::time_of_day>>(meta.value_offset(index), serializer::read_time_of_day(it, end));
                    break;
                }
                case jogasaki::meta::field_type_kind::time_point: {
                    if(fld.option_unsafe<jogasaki::meta::field_type_kind::time_point>()->with_offset_) {
                        auto [v, o] = serializer::read_time_point_with_offset(it, end);
                        (void) o; // currently ignoring offset
                        ref.set_value<runtime_t<meta::field_type_kind::time_point>>(meta.value_offset(index), v);
                        break;
                    }
                    ref.set_value<runtime_t<meta::field_type_kind::time_point>>(meta.value_offset(index), serializer::read_time_point(it, end));
                    break;
                }
                case jogasaki::meta::field_type_kind::blob: {
                    auto [provider, id, reference_tag] = serializer::read_blob(it, end);
                    ref.set_value<runtime_t<meta::field_type_kind::blob>>(meta.value_offset(index), lob::blob_reference{id, static_cast<lob::lob_data_provider>(provider)});
                    record.get_field_value_info(index).blob_reference_tag_ = reference_tag;
                    break;
                }
                case jogasaki::meta::field_type_kind::clob: {
                    auto [provider, id, reference_tag] = serializer::read_clob(it, end);
                    ref.set_value<runtime_t<meta::field_type_kind::clob>>(meta.value_offset(index), lob::clob_reference{id, static_cast<lob::lob_data_provider>(provider)});
                    record.get_field_value_info(index).blob_reference_tag_ = reference_tag;
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
    meta::record_meta& meta
) {
    std::vector<mock::basic_record> ret{};
    for(auto sv : data) {
        auto res = deserialize_msg(sv, meta);
        // TODO basic_record is move only for now
        ret.insert(ret.end(), std::move_iterator{res.begin()}, std::move_iterator{res.end()});
    }
    return ret;
}

}
