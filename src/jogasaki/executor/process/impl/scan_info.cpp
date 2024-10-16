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
#include "scan_info.h"

#include <utility>

#include <jogasaki/executor/process/impl/ops/details/search_key_field_info.h>
#include <jogasaki/kvs/storage.h>

namespace jogasaki::executor::process::impl {

scan_info::scan_info(
    std::vector<ops::details::search_key_field_info> begin_columns,
    kvs::end_point_kind begin_endpoint,
    std::vector<ops::details::search_key_field_info> end_columns,
    kvs::end_point_kind end_endpoint
) :
    begin_columns_(std::move(begin_columns)),
    begin_endpoint_(begin_endpoint),
    end_columns_(std::move(end_columns)),
    end_endpoint_(end_endpoint)
{}

std::vector<ops::details::search_key_field_info> const& scan_info::begin_columns() const noexcept {
    return begin_columns_;
}

std::vector<ops::details::search_key_field_info> const& scan_info::end_columns() const noexcept {
    return end_columns_;
}

kvs::end_point_kind scan_info::begin_endpoint() const noexcept {
    return begin_endpoint_;
}

kvs::end_point_kind scan_info::end_endpoint() const noexcept {
    return end_endpoint_;
}

[[nodiscard]] data::aligned_buffer & scan_info::key_begin() noexcept {
    return key_begin_;
}

[[nodiscard]] data::aligned_buffer & scan_info::key_end() noexcept {
    return key_end_;
}
void scan_info::blen(std::size_t s) noexcept {
    blen_ = s;
}
void scan_info::elen(std::size_t s) noexcept {
    elen_ = s;
}
[[nodiscard]] std::string_view scan_info::begin_key() const noexcept{
    return {static_cast<char*>(key_begin_.data()), blen_};
}
[[nodiscard]] std::string_view scan_info::end_key() const noexcept{
    return {static_cast<char*>(key_end_.data()), elen_};
}
[[nodiscard]] kvs::end_point_kind scan_info::get_kind(bool use_secondary, kvs::end_point_kind endpoint) const noexcept {
    if (use_secondary) {
        if (endpoint == kvs::end_point_kind::inclusive) {
            return kvs::end_point_kind::prefixed_inclusive;
        }
        if (endpoint == kvs::end_point_kind::exclusive) {
            return kvs::end_point_kind::prefixed_exclusive;
        }
    }
    return endpoint;
}
[[nodiscard]] kvs::end_point_kind scan_info::begin_kind(bool use_secondary) const noexcept{
    return get_kind(use_secondary, begin_endpoint_);
}
[[nodiscard]] kvs::end_point_kind scan_info::end_kind(bool use_secondary) const noexcept{
    return get_kind(use_secondary, end_endpoint_);
}

[[nodiscard]] status scan_info::status_result() const noexcept{
    return status_result_;
}
void scan_info::status_result(status s) noexcept{
    status_result_ = s;
}

void scan_info::dump(std::ostream& out,int indent) const noexcept {
    std::string indent_space(indent, ' ');
    out << indent_space << "scan_info:" << std::endl;

    out << indent_space << "  begin_columns_ size: " << begin_columns_.size() << std::endl;
    for (const auto& col : begin_columns_) {
        col.dump(out,4 + indent);
    }

    out <<  indent_space << "  begin_endpoint_ : " << to_string_end_point_kind(begin_endpoint_) << std::endl;
    out <<  indent_space << "  end_columns_ size: " << end_columns_.size() << std::endl;
    for (const auto& col : end_columns_) {
        col.dump(out,4 + indent);
    }

    out <<  indent_space << "  end_endpoint_: " << to_string_end_point_kind(end_endpoint_) << std::endl;
    out <<  indent_space << "  key_begin_: " << std::endl;
    key_begin_.dump(out,4 + indent);
    out <<  indent_space << "  key_end_: " << std::endl;
    key_end_.dump(out,4 + indent);
    out <<  indent_space << "  blen_: " << blen_ << std::endl;
    out <<  indent_space << "  elen_: " << elen_ << std::endl;
}

}
