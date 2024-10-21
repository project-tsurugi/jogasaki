/*
 * Copyright 2018-2024 Project Tsurugi.
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

#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/executor/process/impl/ops/context_base.h>
#include <jogasaki/executor/process/impl/ops/details/encode_key.h>
#include <jogasaki/executor/process/impl/ops/details/search_key_field_info.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/kvs/storage.h>

namespace jogasaki::executor::process::impl {

    using memory_resource = ops::context_base::memory_resource;
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
{
   varlen_resource_ = std::make_unique<memory_resource>(&global::page_pool());
}

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

[[nodiscard]] std::unique_ptr<memory_resource> scan_info::varlen_resource() noexcept {
    return std::move(varlen_resource_);
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

void scan_info::encode_key(request_context* rc) noexcept {
    std::string msg{};
    executor::process::impl::variable_table vars{};
    if(status_result_ = impl::ops::details::encode_key(
        rc,
        begin_columns_,
        vars,
        *varlen_resource_,
        key_begin_,
        blen_,
        msg
       );
       status_result_ != status::ok) {
        if(status_result_ == status::err_type_mismatch) {
            // only on err_type_mismatch, msg is filled with error message. use it to create the error info in request context
            set_error(*rc, error_code::unsupported_runtime_feature_exception, msg, status_result_);
        }
        return;
    }
    if(status_result_ = impl::ops::details::encode_key(
        rc,
        end_columns_,
        vars,
        *varlen_resource_,
        key_end_,
        elen_,
        msg
       );
       status_result_ != status::ok) {
        if(status_result_ == status::err_type_mismatch) {
            // only on err_type_mismatch, msg is filled with error message. use it to create the error info in request context
            set_error(*rc, error_code::unsupported_runtime_feature_exception, msg, status_result_);
        }
    }
        std::cerr <<  "key_begin_" << std::endl;
        key_begin_.dump(std::cerr,0);
        std::cerr <<  "key_end_" << std::endl;
        key_end_.dump(std::cerr,0);
}

void scan_info::dump(std::ostream& out, int indent) const noexcept{
    std::string indent_space(indent, ' ');
    out << indent_space << "begin_columns_: " << &begin_columns_ << "\n";
    out << indent_space << "begin_endpoint_: " << &begin_endpoint_ << "\n";
    out << indent_space << "end_columns_: " << &end_columns_ << "\n";
    out << indent_space << "end_endpoint_: " << &end_endpoint_ << "\n";
    out << indent_space << "key_begin_: " << &key_begin_ << "\n";
    out << indent_space << "key_end_: " << &key_end_ << "\n";
    out << indent_space << "blen_: " <<  blen_ << "\n";
    out << indent_space << "elen_: " <<  elen_ << "\n";
    out << indent_space << "status_result_: " << &status_result_ << "\n";
    out << indent_space << "varlen_resource_: " << &varlen_resource_ << "\n";
}

} // namespace jogasaki::executor::process::impl
