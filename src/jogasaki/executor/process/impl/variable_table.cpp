/*
 * Copyright 2018-2023 Project Tsurugi.
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
#include "variable_table.h"

#include <jogasaki/accessor/record_printer.h>

namespace jogasaki::executor::process::impl {

variable_table::variable_table(
    variable_table_info const& info
) :
    info_(std::addressof(info)),
    store_(std::make_unique<data::small_record_store>(info.meta()))
{}

data::small_record_store& variable_table::store() const noexcept {
    return *store_;
}

maybe_shared_ptr<meta::record_meta> const& variable_table::meta() const noexcept {
    return info_->meta();
}

variable_table_info const& variable_table::info() const noexcept {
    return *info_;
}

variable_table::operator bool() const noexcept {
    return info_ != nullptr;
}

std::ostream& operator<<(std::ostream& out, variable_table const& value) {
    auto rec = value.store().ref();
    auto meta = value.meta();
    bool is_first = true;
    for(auto b = value.info().name_list_begin(), e = value.info().name_list_end(); b != e; ++b) {
        if(! is_first) {
            out << " ";
        }
        is_first = false;
        auto& name = b->first;
        auto& vinfo = b->second;
        out << name << ":";
        if(rec.is_null(vinfo.nullity_offset())) {
            out << "<null>";
            continue;
        }
        accessor::print_field(out, rec, meta->at(vinfo.index()), vinfo.value_offset());
    }
    return out;
}

}


