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
#pragma once

#include <optional>
#include <string_view>
#include <vector>

#include "plugin_api.h"

namespace plugin::udf {

class column_descriptor_impl : public column_descriptor {
public:

    column_descriptor_impl(
        index_type i,
        std::string_view n,
        type_kind_type k,
        record_descriptor* nested = nullptr,
        std::optional<oneof_index_type> oneof = std::nullopt,
        std::optional<std::string_view> oneof_name_val = std::nullopt
    );

    [[nodiscard]] index_type index() const noexcept override;
    [[nodiscard]] std::string_view column_name() const noexcept override;
    [[nodiscard]] type_kind_type type_kind() const noexcept override;
    [[nodiscard]] record_descriptor* nested() const noexcept override;
    [[nodiscard]] std::optional<oneof_index_type> oneof_index() const noexcept override;
    [[nodiscard]] bool has_oneof() const noexcept override;
    [[nodiscard]] std::optional<std::string_view> oneof_name() const noexcept override;

private:

    index_type _idx;
    std::string_view _name;
    type_kind_type _kind;
    record_descriptor* _nested_record;
    std::optional<oneof_index_type> _oneof_idx;
    std::optional<std::string_view> _oneof_name;
};

class record_descriptor_impl : public record_descriptor {
public:

    record_descriptor_impl(std::string_view n, const std::vector<column_descriptor*>& c);

    [[nodiscard]] std::vector<column_descriptor*> const& columns() const noexcept override;
    [[nodiscard]] std::string_view record_name() const noexcept override;
    [[nodiscard]] std::vector<std::vector<column_descriptor*>> const& argument_patterns() const noexcept override;

private:

    std::string_view _name;
    std::vector<column_descriptor*> _cols;
    std::vector<std::vector<column_descriptor*>> _argument_patterns;
    [[nodiscard]] std::vector<std::vector<column_descriptor*>>
    build_argument_patterns(std::vector<column_descriptor*> const& cols) noexcept;
};

class function_descriptor_impl : public function_descriptor {
public:

    function_descriptor_impl(
        index_type i,
        std::string_view n,
        function_kind_type k,
        record_descriptor_impl* in,
        record_descriptor_impl* out
    );

    [[nodiscard]] index_type function_index() const noexcept override;
    [[nodiscard]] std::string_view function_name() const noexcept override;
    [[nodiscard]] function_kind_type function_kind() const noexcept override;
    [[nodiscard]] record_descriptor const& input_record() const noexcept override;
    [[nodiscard]] record_descriptor const& output_record() const noexcept override;

private:

    index_type _idx;
    std::string_view _name;
    function_kind_type _kind;
    record_descriptor_impl* _input;
    record_descriptor_impl* _output;
};

class service_descriptor_impl : public service_descriptor {
public:

    service_descriptor_impl(index_type i, std::string_view n, std::vector<function_descriptor*> f);

    [[nodiscard]] index_type service_index() const noexcept override;
    [[nodiscard]] std::string_view service_name() const noexcept override;
    [[nodiscard]] std::vector<function_descriptor*> const& functions() const noexcept override;

private:

    index_type _idx;
    std::string_view _name;
    std::vector<function_descriptor*> _funcs;
};

class package_descriptor_impl : public package_descriptor {
public:

    package_descriptor_impl(
        std::string_view name,
        std::string_view file_name,
        package_version version,
        std::vector<service_descriptor*> services
    );

    [[nodiscard]] std::string_view package_name() const noexcept override;
    [[nodiscard]] std::vector<service_descriptor*> const& services() const noexcept override;
    [[nodiscard]] std::string_view file_name() const noexcept override;
    [[nodiscard]] package_version version() const noexcept override;

private:

    std::string_view _name;
    std::string_view _file_name;
    package_version _version;
    std::vector<service_descriptor*> _svcs;
};
}  // namespace plugin::udf
