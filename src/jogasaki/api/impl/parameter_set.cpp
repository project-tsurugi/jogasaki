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
#include "parameter_set.h"

#include <utility>

#include <jogasaki/accessor/text.h>
#include <jogasaki/api/parameter_set.h>
#include <jogasaki/plan/parameter_set.h>

namespace jogasaki::api::impl {

parameter_set::parameter_set(std::shared_ptr<plan::parameter_set> body) noexcept: body_(std::move(body)) {}

void parameter_set::set_boolean(std::string_view name, field_type_traits<kind::boolean>::runtime_type value) {
    body_->set_boolean(name, value);
}

void parameter_set::set_int4(std::string_view name, field_type_traits<kind::int4>::runtime_type value) {
    body_->set_int4(name, value);
}

void parameter_set::set_int8(std::string_view name, field_type_traits<kind::int8>::runtime_type value) {
    body_->set_int8(name, value);
}

void parameter_set::set_float4(std::string_view name, field_type_traits<kind::float4>::runtime_type value) {
    body_->set_float4(name, value);
}

void parameter_set::set_float8(std::string_view name, field_type_traits<kind::float8>::runtime_type value) {
    body_->set_float8(name, value);
}

void parameter_set::set_character(std::string_view name, field_type_traits<kind::character>::runtime_type value) {
    body_->set_character(name, accessor::text{const_cast<char*>(value.data()), value.size()});
}

void parameter_set::set_octet(std::string_view name, field_type_traits<kind::octet>::runtime_type value) {
    body_->set_octet(name, accessor::binary{const_cast<char*>(value.data()), value.size()});
}

void parameter_set::set_decimal(std::string_view name, field_type_traits<kind::decimal>::runtime_type value) {
    body_->set_decimal(name, value);
}

void parameter_set::set_date(std::string_view name, field_type_traits<kind::date>::runtime_type value) {
    body_->set_date(name, value);
}

void parameter_set::set_time_of_day(std::string_view name, field_type_traits<kind::time_of_day>::runtime_type value) {
    body_->set_time_of_day(name, value);
}

void parameter_set::set_time_point(std::string_view name, field_type_traits<kind::time_point>::runtime_type value) {
    body_->set_time_point(name, value);
}

void parameter_set::set_blob(std::string_view name, field_type_traits<kind::blob>::parameter_type value) {
    body_->set_blob(name, value);
}

void parameter_set::set_clob(std::string_view name, field_type_traits<kind::clob>::parameter_type value) {
    body_->set_clob(name, value);
}

void parameter_set::set_reference_column(std::string_view name, std::size_t position) {
    body_->set_reference_column(name, position);
}

void parameter_set::set_reference_column(std::string_view name, std::string_view column_name) {
    body_->set_reference_column(name, column_name);
}

void parameter_set::set_null(std::string_view name) {
    body_->set_null(name);
}

parameter_set* parameter_set::clone() const& {
    return new parameter_set(std::make_shared<plan::parameter_set>(*body_));
}

parameter_set* parameter_set::clone() && {
    return new parameter_set(std::make_shared<plan::parameter_set>(std::move(*body_)));
}

std::shared_ptr<plan::parameter_set> const& parameter_set::body() const noexcept {
    return body_;
}

}

namespace jogasaki::api {

std::unique_ptr<parameter_set> create_parameter_set() {
    return std::make_unique<impl::parameter_set>();
}

}
