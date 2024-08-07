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

#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

#include <jogasaki/accessor/record_printer.h>
#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/meta/record_meta.h>

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

void print(
    std::ostream& out,
    bool& is_first,
    std::string_view name,
    accessor::record_ref rec,
    meta::record_meta const& meta,
    value_info const& vinfo
) {
    if(! is_first) { out << " "; }
    is_first = false;
    out << name << ":";
    if(rec.is_null(vinfo.nullity_offset())) {
        out << "<null>";
        return;
    }
    accessor::print_field(out, rec, meta.at(vinfo.index()), vinfo.value_offset());
}
std::ostream& operator<<(std::ostream& out, variable_table const& value) {
    auto rec = value.store().ref();
    auto& meta = value.meta();
    bool is_first = true;
    if(value.info().name_list_empty()) {
        // no name for field, so use "#0", "#1", ... for field name
        auto cnt = 0;
        for(auto b = value.info().variable_list_begin(), e = value.info().variable_list_end(); b != e; ++b) {
            print(out, is_first, "#"+std::to_string(cnt), rec, *meta, b->second);
            ++cnt;
        }
    } else {
        for(auto b = value.info().name_list_begin(), e = value.info().name_list_end(); b != e; ++b) {
            print(out, is_first, b->first, rec, *meta, b->second);
        }
    }
    return out;
}

void variable_table::dump(std::string const& indent) const noexcept {
    std::cerr << indent << "variable_table:\n"
        << indent << "  " << std::left << std::setw(18) << "info:"
        << std::hex << (info_ ? info_ : nullptr) << "\n"
        << indent << "  " << std::setw(18) << "store:"
        << (store_ ? store_.get() : nullptr) << std::endl;
    if (store_) {
        std::cerr << indent << "  " << std::setw(18)
            << "store value:" << *store_ << std::endl;
    }
}

}


