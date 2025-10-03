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
    ) :
        _idx(i),
        _name(n),
        _kind(k),
        _nested_record(nested),
        _oneof_idx(oneof),
        _oneof_name(oneof_name_val) {}

    index_type index() const noexcept override { return _idx; }
    std::string_view column_name() const noexcept override { return _name; }
    type_kind_type type_kind() const noexcept override { return _kind; }
    record_descriptor* nested() const noexcept override { return _nested_record; }
    std::optional<oneof_index_type> oneof_index() const noexcept override { return _oneof_idx; }
    [[nodiscard]] bool has_oneof() const noexcept override { return _oneof_idx.has_value(); }
    std::optional<std::string_view> oneof_name() const noexcept override { return _oneof_name; }

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

    record_descriptor_impl(std::string_view n, std::vector<column_descriptor*> c) : _name(n), _cols(std::move(c)) {}
    const std::vector<column_descriptor*>& columns() const noexcept override { return _cols; }
    std::string_view record_name() const noexcept override { return _name; }

private:

    std::string_view _name;
    std::vector<column_descriptor*> _cols;
};

class function_descriptor_impl : public function_descriptor {
public:

    function_descriptor_impl(
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
    index_type function_index() const noexcept override { return _idx; }
    std::string_view function_name() const noexcept override { return _name; }
    function_kind_type function_kind() const noexcept override { return _kind; }
    const record_descriptor& input_record() const noexcept override { return *_input; }
    const record_descriptor& output_record() const noexcept override { return *_output; }

private:

    index_type _idx;
    std::string_view _name;
    function_kind_type _kind;
    record_descriptor_impl* _input;
    record_descriptor_impl* _output;
};

class service_descriptor_impl : public service_descriptor {
public:

    service_descriptor_impl(index_type i, std::string_view n, std::vector<function_descriptor*> f) :
        _idx(i),
        _name(n),
        _funcs(std::move(f)) {}
    index_type service_index() const noexcept override { return _idx; }
    std::string_view service_name() const noexcept override { return _name; }
    const std::vector<function_descriptor*>& functions() const noexcept override { return _funcs; }

private:

    index_type _idx;
    std::string_view _name;
    std::vector<function_descriptor*> _funcs;
};

class package_descriptor_impl : public package_descriptor {
public:

    package_descriptor_impl(std::string_view n, std::vector<service_descriptor*> s) : _name(n), _svcs(std::move(s)) {}
    std::string_view package_name() const noexcept override { return _name; }
    const std::vector<service_descriptor*>& services() const noexcept override { return _svcs; }

private:

    std::string_view _name;
    std::vector<service_descriptor*> _svcs;
};

}  // namespace plugin::udf
