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
#include "to_common_columns.h"

#include <glog/logging.h>

#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/meta/character_field_option.h>
#include <jogasaki/meta/decimal_field_option.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/octet_field_option.h>

namespace jogasaki::executor {

std::vector<dto::common_column> to_common_columns(meta::external_record_meta const& meta) {
    using t = meta::field_type_kind;
    using atom_type = dto::common_column::atom_type;
    std::vector<dto::common_column> ret{};
    std::size_t n = meta.field_count();
    ret.reserve(n);
    for (std::size_t i = 0; i < n; i++) {
        auto&& column = ret.emplace_back();
        if(auto name = meta.field_name(i); name.has_value()) {
            column.name_ = *name;
        }
        auto& fld = meta.at(i);
        switch(fld.kind()) {
            case t::boolean:
                column.atom_type_ = atom_type::boolean;
                break;
            case t::int4:
                column.atom_type_ = atom_type::int4;
                break;
            case t::int8:
                column.atom_type_ = atom_type::int8;
                break;
            case t::float4:
                column.atom_type_ = atom_type::float4;
                break;
            case t::float8:
                column.atom_type_ = atom_type::float8;
                break;
            case t::decimal:
                column.atom_type_ = atom_type::decimal;
                if(auto const& o = fld.option_unsafe<t::decimal>()) {
                    if(o->precision_){
                        column.precision_ = static_cast<std::uint32_t>(*o->precision_);
                    } else {
                        column.precision_ = true;
                    }
                    if(o->scale_) {
                        column.scale_ = static_cast<std::uint32_t>(*o->scale_);
                    } else {
                        column.scale_ = true;
                    }
                }
                break;
            case t::character:
                column.atom_type_ = atom_type::character;
                if(auto const& o = fld.option_unsafe<t::character>()) {
                    column.varying_ = o->varying_;
                    if(o->length_) {
                        column.length_ = static_cast<std::uint32_t>(*o->length_);
                    } else {
                        column.length_ = true;
                    }
                }
                break;
            case t::octet:
                column.atom_type_ = atom_type::octet;
                if(auto const& o = fld.option_unsafe<t::octet>()) {
                    column.varying_ = o->varying_;
                    if(o->length_) {
                        column.length_ = static_cast<std::uint32_t>(*o->length_);
                    } else {
                        column.length_ = true;
                    }
                }
                break;
            case t::date:
                column.atom_type_ = atom_type::date;
                break;
            case t::time_of_day:
                BOOST_ASSERT(fld.option_unsafe<t::time_of_day>() != nullptr);  //NOLINT
                if(fld.option_unsafe<t::time_of_day>()->with_offset_) {
                    column.atom_type_ = atom_type::time_of_day_with_time_zone;
                    break;
                }
                column.atom_type_ = atom_type::time_of_day;
                break;
            case t::time_point:
                BOOST_ASSERT(fld.option_unsafe<t::time_point>() != nullptr);  //NOLINT
                if(fld.option_unsafe<t::time_point>()->with_offset_) {
                    column.atom_type_ = atom_type::time_point_with_time_zone;
                    break;
                }
                column.atom_type_ = atom_type::time_point;
                break;
            case t::blob:
                column.atom_type_ = atom_type::blob;
                break;
            case t::clob:
                column.atom_type_ = atom_type::clob;
                break;
            case t::unknown:
                column.atom_type_ = atom_type::unknown;
                break;
            default:
                LOG_LP(ERROR) << "unsupported data type at field (" << i << "): " << fld.kind();
                break;
        }
    }
    return ret;
}

}  // namespace jogasaki::executor
