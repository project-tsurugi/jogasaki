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

using takatori::util::fail;

template <auto Kind>
using traits = meta::field_type_traits<Kind>;

/**
 * @brief print field value to output stream
 * @param os the target output stream to write
 * @param record the record that the field belongs to
 * @param type filed type of the target
 * @param offset offset of the target field in the contained record
 */
inline void print_field(
    std::ostream& os,
    accessor::record_ref record,
    meta::field_type const& type,
    meta::record_meta::value_offset_type offset
) {
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
            fail();
    }
}

/**
 * @brief debug support to print record content
 * @details helper class to override operator<< with record_ref and objects and print record content using record_meta.
 * The object of this class is not explicitly constructed. Only operator << generates the instance implicitly.
 * The output format is (index:type)[content] separated by a space, e.g. (0:int8)[100] (1:double)[1.1] (2:text)[ABC]
 */
class record_printer {
public:
    /**
     * @brief receive record metadata and finish writing to stream
     * @param meta record metadata of the record that is written to stream
     * @return the stream originally passed to operator<<(std::ostream& os, record_ref rec)
     */
    std::ostream& operator<<(meta::record_meta const& meta) {
        if (! record_) {
            stream_ << "<null record>";
            return stream_;
        }
        for(std::size_t idx = 0, n = meta.field_count(); idx < n; ++idx) {
            auto nullable = meta.nullable(idx);
            auto is_null = nullable && record_.is_null(meta.nullity_offset(idx));
            if (idx > 0) {
                stream_ << " ";
            }
            stream_ << "(";
            stream_ << idx;
            stream_ << ":";
            stream_ << meta[idx].kind();
            if (nullable) {
                stream_ << "*";
            }
            stream_ << ")[";
            if (is_null) {
                stream_ << "-";
            } else {
                print_field(stream_, record_, meta[idx], meta.value_offset(idx));
            }
            stream_ << "]";
        }
        return stream_;
    }

private:
    constexpr record_printer(std::ostream& stream, record_ref record) noexcept : stream_(stream), record_(record) {}

    std::ostream& stream_;
    record_ref record_;

    friend constexpr record_printer operator<<(std::ostream& os, record_ref rec) noexcept;
};

/**
 * @brief pass record to stream and retrieve helper printer class
 * @param os stream to write to
 * @param rec record whose content should be written
 * @return helper printer class, which finishes writing with operator<<(meta::record_meta const& meta)
 * @attention until returned value's operator<< is called, nothing happens or written to the stream
 */
inline constexpr record_printer operator<<(std::ostream& os, record_ref rec) noexcept {
    return record_printer(os, rec);
}

}
