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
#include "plugin_api.h"
#include <string_view>
#include <vector>
namespace plugin::udf {
class column_descriptor_impl : public column_descriptor {
  public:
    column_descriptor_impl(
        index_type i, std::string_view n, type_kind_type k, record_descriptor* nested = nullptr)
        : idx(i), name(n), kind(k), nested_record(nested) {}

    index_type index() const noexcept override { return idx; }
    std::string_view column_name() const noexcept override { return name; }
    type_kind_type type_kind() const noexcept override { return kind; }
    record_descriptor* nested() const noexcept { return nested_record; }

  private:
    index_type idx;
    std::string_view name;
    type_kind_type kind;
    record_descriptor* nested_record;
};

class record_descriptor_impl : public record_descriptor {
  public:
    record_descriptor_impl(std::string_view n, std::vector<column_descriptor*> c)
        : name(n), cols(std::move(c)) {}
    const std::vector<column_descriptor*>& columns() const noexcept override { return cols; }
    std::string_view record_name() const noexcept override { return name; }

  private:
    std::string_view name;
    std::vector<column_descriptor*> cols;
};

class function_descriptor_impl : public function_descriptor {
  public:
    function_descriptor_impl(index_type i, std::string_view n, function_kind_type k,
        record_descriptor_impl* in, record_descriptor_impl* out)
        : idx(i), name(n), kind(k), input(in), output(out) {}
    index_type function_index() const noexcept override { return idx; }
    std::string_view function_name() const noexcept override { return name; }
    function_kind_type function_kind() const noexcept override { return kind; }
    const record_descriptor& input_record() const noexcept override { return *input; }
    const record_descriptor& output_record() const noexcept override { return *output; }

  private:
    index_type idx;
    std::string_view name;
    function_kind_type kind;
    record_descriptor_impl* input;
    record_descriptor_impl* output;
};

class service_descriptor_impl : public service_descriptor {
  public:
    service_descriptor_impl(index_type i, std::string_view n, std::vector<function_descriptor*> f)
        : idx(i), name(n), funcs(std::move(f)) {}
    index_type service_index() const noexcept override { return idx; }
    std::string_view service_name() const noexcept override { return name; }
    const std::vector<function_descriptor*>& functions() const noexcept override { return funcs; }

  private:
    index_type idx;
    std::string_view name;
    std::vector<function_descriptor*> funcs;
};

class package_descriptor_impl : public package_descriptor {
  public:
    package_descriptor_impl(std::string_view n, std::vector<service_descriptor*> s)
        : name(n), svcs(std::move(s)) {}
    std::string_view package_name() const noexcept override { return name; }
    const std::vector<service_descriptor*>& services() const noexcept override { return svcs; }

  private:
    std::string_view name;
    std::vector<service_descriptor*> svcs;
};

} // namespace plugin::udf
