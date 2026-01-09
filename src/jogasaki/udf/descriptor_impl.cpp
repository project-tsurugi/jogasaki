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
#include "descriptor_impl.h"

#include <algorithm>
#include <iostream>
#include <optional>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "plugin_api.h"

namespace plugin::udf {

// column_descriptor_impl
column_descriptor_impl::column_descriptor_impl(
    index_type i,
    std::string_view n,
    type_kind_type k,
    record_descriptor* nested,
    std::optional<column_descriptor::oneof_index_type> oneof,
    std::optional<std::string_view> oneof_name_val
) :
    _idx(i),
    _name(n),
    _kind(k),
    _nested_record(nested),
    _oneof_idx(oneof),
    _oneof_name(oneof_name_val) {}

column_descriptor::index_type column_descriptor_impl::index() const noexcept { return _idx; }
std::string_view column_descriptor_impl::column_name() const noexcept { return _name; }
type_kind column_descriptor_impl::type_kind() const noexcept { return _kind; }
record_descriptor* column_descriptor_impl::nested() const noexcept { return _nested_record; }
std::optional<column_descriptor::oneof_index_type> column_descriptor_impl::oneof_index() const noexcept {
    return _oneof_idx;
}
bool column_descriptor_impl::has_oneof() const noexcept { return _oneof_idx.has_value(); }
std::optional<std::string_view> column_descriptor_impl::oneof_name() const noexcept { return _oneof_name; }

// record_descriptor_impl
record_descriptor_impl::record_descriptor_impl(std::string_view n, const std::vector<column_descriptor*>& c) :
    _name(n),
    _cols(c),
    _argument_patterns(build_argument_patterns(_cols)) {}

std::vector<column_descriptor*> const& record_descriptor_impl::columns() const noexcept { return _cols; }
std::string_view record_descriptor_impl::record_name() const noexcept { return _name; }
std::vector<std::vector<column_descriptor*>> const& record_descriptor_impl::argument_patterns() const noexcept {
    return _argument_patterns;
}

std::vector<std::vector<column_descriptor*>>
record_descriptor_impl::build_argument_patterns(std::vector<column_descriptor*> const& cols) noexcept {
    std::vector<std::vector<column_descriptor*>> patterns(1);
    std::unordered_map<column_descriptor::oneof_index_type, std::vector<column_descriptor*>> oneof_groups;

    for(auto* col: cols) {
        if(col->has_oneof()) { oneof_groups[*col->oneof_index()].push_back(col); }
    }

    std::unordered_map<column_descriptor::oneof_index_type, bool> processed;

    for(auto* col: cols) {
        if(! col->has_oneof()) {
            for(auto& p: patterns) { p.push_back(col); }
        } else {
            auto idx = *col->oneof_index();
            if(processed[idx]) continue;
            processed[idx] = true;

            auto& group = oneof_groups[idx];
            std::vector<std::vector<column_descriptor*>> next_patterns;

            for(auto& p: patterns) {
                for(auto* choice: group) {
                    auto q = p;
                    q.push_back(choice);
                    next_patterns.push_back(q);
                }
            }

            patterns.swap(next_patterns);
        }
    }

    return patterns;
}

// function_descriptor_impl
function_descriptor_impl::function_descriptor_impl(
    index_type i,
    std::string_view n,
    function_kind_type k,
    record_descriptor_impl* in,
    record_descriptor_impl* out
) :
    _idx(i),
    _name(n),
    _kind(k),
    _input(in),
    _output(out) {}

function_descriptor::index_type function_descriptor_impl::function_index() const noexcept { return _idx; }
std::string_view function_descriptor_impl::function_name() const noexcept { return _name; }
function_kind function_descriptor_impl::function_kind() const noexcept { return _kind; }
record_descriptor const& function_descriptor_impl::input_record() const noexcept { return *_input; }
record_descriptor const& function_descriptor_impl::output_record() const noexcept { return *_output; }

// service_descriptor_impl
service_descriptor_impl::service_descriptor_impl(
    index_type i,
    std::string_view n,
    std::vector<function_descriptor*> f
) :
    _idx(i),
    _name(n),
    _funcs(std::move(f)) {}

service_descriptor::index_type service_descriptor_impl::service_index() const noexcept { return _idx; }
std::string_view service_descriptor_impl::service_name() const noexcept { return _name; }
std::vector<function_descriptor*> const& service_descriptor_impl::functions() const noexcept { return _funcs; }

// package_descriptor_impl
package_descriptor_impl::package_descriptor_impl(
    std::string_view name,
    std::string_view file_name,
    package_version version,
    std::vector<service_descriptor*> services
) :
    _name(name),
    _file_name(file_name),
    _version(version),
    _svcs(std::move(services)) {}

std::string_view package_descriptor_impl::package_name() const noexcept { return _name; }
std::vector<service_descriptor*> const& package_descriptor_impl::services() const noexcept { return _svcs; }
std::string_view package_descriptor_impl::file_name() const noexcept { return _file_name; }
package_version package_descriptor_impl::version() const noexcept { return _version; }
}  // namespace plugin::udf
