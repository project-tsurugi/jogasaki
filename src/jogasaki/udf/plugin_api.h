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
#include "enum_types.h"
#include "generic_record.h"
#include "generic_record_impl.h"
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace plugin::udf {

[[nodiscard]] std::string to_string(function_kind_type kind);
[[nodiscard]] std::string to_string(type_kind_type kind);
class record_descriptor;
class column_descriptor {
  public:
    using index_type                                                    = std::size_t;
    virtual ~column_descriptor()                                        = default;
    column_descriptor()                                                 = default;
    column_descriptor(const column_descriptor&)                         = delete;
    column_descriptor& operator=(const column_descriptor&)              = delete;
    column_descriptor(column_descriptor&&)                              = delete;
    column_descriptor& operator=(column_descriptor&&)                   = delete;
    [[nodiscard]] virtual index_type index() const noexcept             = 0;
    [[nodiscard]] virtual std::string_view column_name() const noexcept = 0;
    [[nodiscard]] virtual type_kind_type type_kind() const noexcept     = 0;
    [[nodiscard]] virtual record_descriptor* nested() const noexcept    = 0;
};

class record_descriptor {
  public:
    record_descriptor()                                                                   = default;
    virtual ~record_descriptor()                                                          = default;
    record_descriptor(const record_descriptor&)                                           = delete;
    record_descriptor& operator=(const record_descriptor&)                                = delete;
    record_descriptor(record_descriptor&&)                                                = delete;
    record_descriptor& operator=(record_descriptor&&)                                     = delete;
    [[nodiscard]] virtual std::string_view record_name() const noexcept                   = 0;
    [[nodiscard]] virtual const std::vector<column_descriptor*>& columns() const noexcept = 0;
};

class function_descriptor {
  public:
    function_descriptor()                                      = default;
    using index_type                                           = std::size_t;
    virtual ~function_descriptor()                             = default;
    function_descriptor(const function_descriptor&)            = delete;
    function_descriptor& operator=(const function_descriptor&) = delete;
    function_descriptor(function_descriptor&&)                 = delete;
    function_descriptor& operator=(function_descriptor&&)      = delete;

    [[nodiscard]] virtual index_type function_index() const noexcept              = 0;
    [[nodiscard]] virtual std::string_view function_name() const noexcept         = 0;
    [[nodiscard]] virtual function_kind_type function_kind() const noexcept       = 0;
    [[nodiscard]] virtual const record_descriptor& input_record() const noexcept  = 0;
    [[nodiscard]] virtual const record_descriptor& output_record() const noexcept = 0;
};

class service_descriptor {
  public:
    service_descriptor()                                                 = default;
    using index_type                                                     = std::size_t;
    virtual ~service_descriptor()                                        = default;
    service_descriptor(const service_descriptor&)                        = delete;
    service_descriptor& operator=(const service_descriptor&)             = delete;
    service_descriptor(service_descriptor&&)                             = delete;
    service_descriptor& operator=(service_descriptor&&)                  = delete;
    [[nodiscard]] virtual index_type service_index() const noexcept      = 0;
    [[nodiscard]] virtual std::string_view service_name() const noexcept = 0;
    [[nodiscard]] virtual const std::vector<function_descriptor*>& functions() const noexcept = 0;
};

class package_descriptor {
  public:
    package_descriptor()                                                 = default;
    virtual ~package_descriptor()                                        = default;
    package_descriptor(const package_descriptor&)                        = delete;
    package_descriptor& operator=(const package_descriptor&)             = delete;
    package_descriptor(package_descriptor&&)                             = delete;
    package_descriptor& operator=(package_descriptor&&)                  = delete;
    [[nodiscard]] virtual std::string_view package_name() const noexcept = 0;
    [[nodiscard]] virtual const std::vector<service_descriptor*>& services() const noexcept = 0;
};

class plugin_api {
  public:
    plugin_api()                             = default;
    virtual ~plugin_api()                    = default;
    plugin_api(const plugin_api&)            = delete;
    plugin_api& operator=(const plugin_api&) = delete;
    plugin_api(plugin_api&&)                 = delete;
    plugin_api& operator=(plugin_api&&)      = delete;
    [[nodiscard]] virtual const std::vector<package_descriptor*>& packages() const noexcept = 0;
};
void print_columns(const std::vector<column_descriptor*>& cols, int indent);
void print_plugin_info(const std::shared_ptr<plugin_api>& api);
[[nodiscard]] std::vector<NativeValue> column_to_native_values(
    const std::vector<column_descriptor*>& cols);
[[nodiscard]] std::vector<NativeValue> cursor_to_native_values(
    generic_record_impl& response, const std::vector<column_descriptor*>& cols);
void print_native_values(const std::vector<NativeValue>& values);
extern "C" plugin_api* create_plugin_api();
} // namespace plugin::udf
