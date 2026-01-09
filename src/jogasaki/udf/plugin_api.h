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
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "enum_types.h"
#include "generic_record.h"
#include "generic_record_impl.h"

namespace plugin::udf {

[[nodiscard]] std::string to_string(function_kind kind);
[[nodiscard]] std::string to_string(type_kind kind);
class package_version final {
public:

    package_version() = default;
    package_version(std::size_t maj, std::size_t min, std::size_t pat) noexcept :
        major_(maj),
        minor_(min),
        patch_(pat) {}

    [[nodiscard]] std::size_t major() const noexcept { return major_; }
    [[nodiscard]] std::size_t minor() const noexcept { return minor_; }
    [[nodiscard]] std::size_t patch() const noexcept { return patch_; }
    bool operator==(package_version const& other) const noexcept {
        return major_ == other.major_ && minor_ == other.minor_ && patch_ == other.patch_;
    }

    bool operator<(package_version const& other) const noexcept {
        if(major_ != other.major_) return major_ < other.major_;
        if(minor_ != other.minor_) return minor_ < other.minor_;
        return patch_ < other.patch_;
    }

private:

    std::size_t major_{0};
    std::size_t minor_{0};
    std::size_t patch_{0};
};

class record_descriptor;
class column_descriptor {
public:
    using type_kind_type = plugin::udf::type_kind;
    using index_type = std::size_t;
    using oneof_index_type = std::size_t;
    virtual ~column_descriptor() = default;
    column_descriptor() = default;
    column_descriptor(column_descriptor const&) = delete;
    column_descriptor& operator=(column_descriptor const&) = delete;
    column_descriptor(column_descriptor&&) = delete;
    column_descriptor& operator=(column_descriptor&&) = delete;
    [[nodiscard]] virtual index_type index() const noexcept = 0;
    [[nodiscard]] virtual std::string_view column_name() const noexcept = 0;
    [[nodiscard]] virtual type_kind_type type_kind() const noexcept = 0;
    [[nodiscard]] virtual record_descriptor* nested() const noexcept = 0;
    [[nodiscard]] virtual bool has_oneof() const noexcept = 0;
    [[nodiscard]] virtual std::optional<oneof_index_type> oneof_index() const noexcept = 0;
    [[nodiscard]] virtual std::optional<std::string_view> oneof_name() const noexcept = 0;
};
class record_descriptor {
public:

    record_descriptor() = default;
    virtual ~record_descriptor() = default;
    record_descriptor(record_descriptor const&) = delete;
    record_descriptor& operator=(record_descriptor const&) = delete;
    record_descriptor(record_descriptor&&) = delete;
    record_descriptor& operator=(record_descriptor&&) = delete;
    [[nodiscard]] virtual std::string_view record_name() const noexcept = 0;
    [[nodiscard]] virtual const std::vector<column_descriptor*>& columns() const noexcept = 0;
    [[nodiscard]] virtual const std::vector<std::vector<column_descriptor*>>& argument_patterns() const noexcept = 0;
};

class function_descriptor {
public:
    using function_kind_type = plugin::udf::function_kind;
    function_descriptor() = default;
    using index_type = std::size_t;
    virtual ~function_descriptor() = default;
    function_descriptor(function_descriptor const&) = delete;
    function_descriptor& operator=(function_descriptor const&) = delete;
    function_descriptor(function_descriptor&&) = delete;
    function_descriptor& operator=(function_descriptor&&) = delete;

    [[nodiscard]] virtual index_type function_index() const noexcept = 0;
    [[nodiscard]] virtual std::string_view function_name() const noexcept = 0;
    [[nodiscard]] virtual function_kind_type function_kind() const noexcept = 0;
    [[nodiscard]] virtual record_descriptor const& input_record() const noexcept = 0;
    [[nodiscard]] virtual record_descriptor const& output_record() const noexcept = 0;
};

class service_descriptor {
public:

    service_descriptor() = default;
    using index_type = std::size_t;
    virtual ~service_descriptor() = default;
    service_descriptor(service_descriptor const&) = delete;
    service_descriptor& operator=(service_descriptor const&) = delete;
    service_descriptor(service_descriptor&&) = delete;
    service_descriptor& operator=(service_descriptor&&) = delete;
    [[nodiscard]] virtual index_type service_index() const noexcept = 0;
    [[nodiscard]] virtual std::string_view service_name() const noexcept = 0;
    [[nodiscard]] virtual std::vector<function_descriptor*> const& functions() const noexcept = 0;
};

class package_descriptor {
public:

    package_descriptor() = default;
    virtual ~package_descriptor() = default;
    package_descriptor(package_descriptor const&) = delete;
    package_descriptor& operator=(package_descriptor const&) = delete;
    package_descriptor(package_descriptor&&) = delete;
    package_descriptor& operator=(package_descriptor&&) = delete;
    [[nodiscard]] virtual std::string_view package_name() const noexcept = 0;
    [[nodiscard]] virtual std::string_view file_name() const noexcept = 0;
    [[nodiscard]] virtual package_version version() const noexcept = 0;
    [[nodiscard]] virtual std::vector<service_descriptor*> const& services() const noexcept = 0;
};

class plugin_api {
public:

    plugin_api() = default;
    virtual ~plugin_api() = default;
    plugin_api(plugin_api const&) = delete;
    plugin_api& operator=(plugin_api const&) = delete;
    plugin_api(plugin_api&&) = delete;
    plugin_api& operator=(plugin_api&&) = delete;
    [[nodiscard]] virtual std::vector<package_descriptor*> const& packages() const noexcept = 0;
};
void print_columns(std::vector<column_descriptor*> const& cols, int indent);
void print_plugin_info(std::shared_ptr<plugin_api> const& api);
extern "C" plugin_api* create_plugin_api();
std::ostream& operator<<(std::ostream& out, type_kind const& kind);
std::ostream& operator<<(std::ostream& out, function_kind const& kind);
}  // namespace plugin::udf
