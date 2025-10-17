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
#include "plugin_api.h"

#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "enum_types.h"
#include "generic_record.h"
#include "generic_record_impl.h"

namespace plugin::udf {
namespace {
std::string_view to_string_view(function_kind_type kind) {
    using namespace std::literals;
    switch(kind) {
        case function_kind_type::Unary: return "Unary"sv;
        case function_kind_type::ClientStreaming: return "ClientStreaming"sv;
        case function_kind_type::ServerStreaming: return "ServerStreaming"sv;
        case function_kind_type::BidirectionalStreaming: return "BidirectionalStreaming"sv;
        default: return "UnknownFunctionKind"sv;
    }
}

std::string_view to_string_view(type_kind_type kind) {
    using namespace std::literals;
    switch(kind) {
        case type_kind_type::FLOAT8: return "FLOAT8"sv;
        case type_kind_type::FLOAT4: return "FLOAT4"sv;
        case type_kind_type::INT8: return "INT8"sv;
        case type_kind_type::UINT8: return "UINT8"sv;
        case type_kind_type::INT4: return "INT4"sv;
        case type_kind_type::FIXED8: return "FIXED8"sv;
        case type_kind_type::FIXED4: return "FIXED4"sv;
        case type_kind_type::BOOL: return "BOOL"sv;
        case type_kind_type::STRING: return "STRING"sv;
        case type_kind_type::GROUP: return "GROUP"sv;
        case type_kind_type::MESSAGE: return "MESSAGE"sv;
        case type_kind_type::BYTES: return "BYTES"sv;
        case type_kind_type::UINT4: return "UINT4"sv;
        case type_kind_type::ENUM: return "ENUM"sv;
        case type_kind_type::SINT4: return "SINT4"sv;
        case type_kind_type::SINT8: return "SINT8"sv;
        case type_kind_type::SFIXED8: return "SFIXED8"sv;
        case type_kind_type::SFIXED4: return "SFIXED4"sv;
        default: return "UnknownTypeKind"sv;
    }
}

void add_column(const std::vector<column_descriptor*>& cols) {
    for(const auto* col: cols) {
        std::cout << "- column_name: " << col->column_name() << std::endl;
        std::cout << "  type_kind: " << plugin::udf::to_string_view(col->type_kind()) << std::endl;

        if(auto nested = col->nested()) {
            std::cout << "  nested_record:" << std::endl;
            std::cout << "    record_name: " << nested->record_name() << std::endl;
            std::cout << "    columns:" << std::endl;
            add_column(nested->columns());
        }
    }
}
}  // anonymous namespace

void print_columns(const std::vector<column_descriptor*>& cols, int indent = 0) {
    std::string indent_str(indent, ' ');

    for(const auto* col: cols) {
        std::cout << indent_str << "- column_name: " << col->column_name() << std::endl;
        std::cout << indent_str << "  type_kind: " << plugin::udf::to_string_view(col->type_kind()) << std::endl;

        if(auto nested = col->nested()) {
            std::cout << indent_str << "  nested_record:" << std::endl;
            std::cout << indent_str << "    record_name: " << nested->record_name() << std::endl;
            std::cout << indent_str << "    columns:" << std::endl;
            print_columns(nested->columns(), indent + 6);
        }
    }
}

void print_plugin_info(const std::shared_ptr<plugin_api>& api) {
    const auto& pkgs = api->packages();
    for(const auto* pkg: pkgs) {
        std::cout << "  - package_name: " << pkg->package_name() << std::endl;
        std::cout << "    services:" << std::endl;
        for(const auto* svc: pkg->services()) {
            std::cout << "      - service_name: " << svc->service_name() << std::endl;
            std::cout << "        service_index: " << svc->service_index() << std::endl;
            std::cout << "        functions:" << std::endl;

            for(const auto* fn: svc->functions()) {
                std::cout << "          - function_name: " << fn->function_name() << std::endl;
                std::cout << "            function_index: " << fn->function_index() << std::endl;
                std::cout << "            function_kind: " << plugin::udf::to_string_view(fn->function_kind())
                          << std::endl;

                const auto& input = fn->input_record();
                std::cout << "            input_record:" << std::endl;
                std::cout << "              record_name: " << input.record_name() << std::endl;
                std::cout << "              columns:" << std::endl;
                print_columns(input.columns(), 16);

                const auto& output = fn->output_record();
                std::cout << "            output_record:" << std::endl;
                std::cout << "              record_name: " << output.record_name() << std::endl;
                std::cout << "              columns:" << std::endl;
                print_columns(output.columns(), 16);
            }
        }
    }
}
}  // namespace plugin::udf
