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

#include <takatori/util/fail.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/meta/record_meta.h>

namespace jogasaki::accessor {

template <auto Kind>
using traits = meta::field_type_traits<Kind>;

/**
 * @brief print field value to output stream
 * @param os the target output stream to write
 * @param record the record that the field belongs to
 * @param type filed type of the target
 * @param offset offset of the target field in the contained record
 */
void print_field(std::ostream& os, accessor::record_ref record, meta::field_type const& type, meta::record_meta::value_offset_type offset) {
    using kind = meta::field_type_kind;
    switch(type.kind()) {
        case kind::undefined: os << kind::undefined; break;
        case kind::boolean: os << record.get_value<traits<kind::int8>::runtime_type>(offset); break;
        case kind::int1: os << record.get_value<traits<kind::int1>::runtime_type>(offset); break;
        case kind::int2: os << record.get_value<traits<kind::int2>::runtime_type>(offset); break;
        case kind::int4: os << record.get_value<traits<kind::int4>::runtime_type>(offset); break;
        case kind::int8: os << record.get_value<traits<kind::int8>::runtime_type>(offset); break;
        case kind::float4: os << record.get_value<traits<kind::float4>::runtime_type>(offset); break;
        case kind::float8: os << record.get_value<traits<kind::float8>::runtime_type>(offset); break;
        case kind::decimal: os << record.get_value<traits<kind::decimal>::runtime_type>(offset); break;
        case kind::character:
            os << static_cast<std::string_view>(record.get_value<traits<kind::character>::runtime_type>(offset));
            break;
//        case kind::bit: os << record.get_value<traits<kind::bit>::runtime_type>(offset); break;
        case kind::date: os << record.get_value<traits<kind::date>::runtime_type>(offset); break;
        case kind::time_of_day: os << record.get_value<traits<kind::time_of_day>::runtime_type>(offset); break;
        case kind::time_point: os << record.get_value<traits<kind::time_point>::runtime_type>(offset); break;
//        case kind::time_interval: os << record.get_value<traits<kind::time_interval>::runtime_type>(offset); break;
//        case kind::array: os << record.get_value<traits<kind::array>::runtime_type>(offset); break;
//        case kind::record: os << record.get_value<traits<kind::record>::runtime_type>(offset); break;
//        case kind::unknown: os << record.get_value<traits<kind::unknown>::runtime_type>(offset); break;
//        case kind::row_reference: os << record.get_value<traits<kind::row_reference>::runtime_type>(offset); break;
//        case kind::row_id: os << record.get_value<traits<kind::row_id>::runtime_type>(offset); break;
//        case kind::declared: os << record.get_value<traits<kind::declared>::runtime_type>(offset); break;
//        case kind::extension: os << record.get_value<traits<kind::extension>::runtime_type>(offset); break;
        default:
            takatori::util::fail();
    }
}

/**
 * @brief debug support to print record content
 * The format is (index:type)[content] separted by a space, e.g.
 * (0:int8)[100] (1:double)[1.0] (2:text)[ABC]
 */
class record_printer {
public:
    record_printer(std::ostream& stream, record_ref& record) noexcept : stream_(stream), record_(record) {}

    std::ostream& print_record(meta::record_meta& meta) {
        // TODO handle null
        for(std::size_t idx = 0, n = meta.field_count(); idx < n; ++idx) {
            if (idx > 0) {
                stream_ << " ";
            }
            stream_ << "(";
            stream_ << idx;
            stream_ << ":";
            stream_ << meta[idx].kind();
            stream_ << ")";
            stream_ << "[";
            print_field(stream_, record_, meta[idx], meta.value_offset(idx));
            stream_ << "]";
        }
        stream_.flush();
        return stream_;
    }

private:
    std::ostream& stream_;
    record_ref& record_;
};

inline record_printer operator<<(std::ostream& os, record_ref& rec) noexcept {
    return record_printer(os, rec);
}

inline std::ostream& operator<<(record_printer&& printer, meta::record_meta& meta) {
    return printer.print_record(meta);
}

}
