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
std::string to_string(function_kind_type kind) {
    switch(kind) {
        case function_kind_type::Unary: return "Unary";
        case function_kind_type::ClientStreaming: return "ClientStreaming";
        case function_kind_type::ServerStreaming: return "ServerStreaming";
        case function_kind_type::BidirectionalStreaming: return "BidirectionalStreaming";
        default: return "UnknownFunctionKind";
    }
}

std::string to_string(type_kind_type kind) {
    switch(kind) {
        case type_kind_type::FLOAT8: return "FLOAT8";
        case type_kind_type::FLOAT4: return "FLOAT4";
        case type_kind_type::INT8: return "INT8";
        case type_kind_type::UINT8: return "UINT8";
        case type_kind_type::INT4: return "INT4";
        case type_kind_type::FIXED8: return "FIXED8";
        case type_kind_type::FIXED4: return "FIXED4";
        case type_kind_type::BOOL: return "BOOL";
        case type_kind_type::STRING: return "STRING";
        case type_kind_type::GROUP: return "GROUP";
        case type_kind_type::MESSAGE: return "MESSAGE";
        case type_kind_type::BYTES: return "BYTES";
        case type_kind_type::UINT4: return "UINT4";
        case type_kind_type::ENUM: return "ENUM";
        case type_kind_type::SINT4: return "SINT4";
        case type_kind_type::SINT8: return "SINT8";
        case type_kind_type::SFIXED8: return "SFIXED8";
        case type_kind_type::SFIXED4: return "SFIXED4";
        default: return "UnknownTypeKind";
    }
}
namespace {
void add_column(const std::vector<column_descriptor*>& cols) {
    for(const auto* col: cols) {
        std::cout << "- column_name: " << col->column_name() << std::endl;
        std::cout << "  type_kind: " << plugin::udf::to_string(col->type_kind()) << std::endl;

        if(auto nested = col->nested()) {
            std::cout << "  nested_record:" << std::endl;
            std::cout << "    record_name: " << nested->record_name() << std::endl;
            std::cout << "    columns:" << std::endl;
            add_column(nested->columns());
        }
    }
}
}  // anonymous namespace
std::vector<NativeValue> column_to_native_values(const std::vector<column_descriptor*>& cols) {
    std::vector<NativeValue> result;

    for(const auto* col: cols) {
        switch(col->type_kind()) {
            case type_kind_type::FLOAT8: result.emplace_back(2.2); break;
            case type_kind_type::FLOAT4: result.emplace_back(1.1F); break;
            case type_kind_type::INT8: result.emplace_back(static_cast<int64_t>(64)); break;
            case type_kind_type::UINT8: result.emplace_back(static_cast<uint64_t>(65)); break;
            case type_kind_type::INT4: result.emplace_back(static_cast<int32_t>(32)); break;
            case type_kind_type::UINT4: result.emplace_back(static_cast<uint32_t>(33)); break;
            case type_kind_type::BOOL: result.emplace_back(false); break;
            case type_kind_type::STRING: result.emplace_back(std::string{"string hello"}); break;
            case type_kind_type::BYTES: result.emplace_back(std::string{"bytes data"}); break;
            case type_kind_type::GROUP:
            case type_kind_type::MESSAGE: {
                if(auto nested_cols = col->nested()) {
                    auto nested_values = column_to_native_values(nested_cols->columns());
                    result.insert(result.end(), nested_values.begin(), nested_values.end());
                } else {
                    result.emplace_back();
                }
                break;
            }

            default: result.emplace_back(); break;
        }
    }

    return result;
}
namespace {

template<typename FetchFunc>
void fetch_and_emplace(std::vector<plugin::udf::NativeValue>& result, type_kind_type kind, FetchFunc&& fetch) {
    if(auto val = std::forward<FetchFunc>(fetch)()) {
        result.emplace_back(*val, kind);
    } else {
        result.emplace_back();
    }
}

}  // anonymous namespace

// @ see  https://protobuf.dev/programming-guides/proto3/#scalar
std::vector<plugin::udf::NativeValue> cursor_to_native_values(
    plugin::udf::generic_record_impl& response,
    const std::vector<plugin::udf::column_descriptor*>& cols
) {
    std::vector<plugin::udf::NativeValue> result;
    if(auto cursor = response.cursor()) {
        for(const auto* col: cols) {
            auto type_kind = col->type_kind();
            switch(type_kind) {
                case type_kind_type::SFIXED4:
                case type_kind_type::INT4:
                case type_kind_type::SINT4:
                    fetch_and_emplace(result, type_kind, [&] { return cursor->fetch_int4(); });
                    break;
                case type_kind_type::SFIXED8:
                case type_kind_type::INT8:
                case type_kind_type::SINT8:
                    fetch_and_emplace(result, type_kind, [&] { return cursor->fetch_int8(); });
                    break;
                case type_kind_type::UINT4:
                case type_kind_type::FIXED4:
                    fetch_and_emplace(result, type_kind, [&] { return cursor->fetch_uint4(); });
                    break;
                case type_kind_type::UINT8:
                case type_kind_type::FIXED8:
                    fetch_and_emplace(result, type_kind, [&] { return cursor->fetch_uint8(); });
                    break;
                case type_kind_type::FLOAT4:
                    fetch_and_emplace(result, type_kind, [&] { return cursor->fetch_float(); });
                    break;
                case type_kind_type::FLOAT8:
                    fetch_and_emplace(result, type_kind, [&] { return cursor->fetch_double(); });
                    break;
                case type_kind_type::BOOL:
                    fetch_and_emplace(result, type_kind, [&] { return cursor->fetch_bool(); });
                    break;
                case type_kind_type::STRING:
                case type_kind_type::BYTES:
                    fetch_and_emplace(result, type_kind, [&] { return cursor->fetch_string(); });
                    break;

                case type_kind_type::GROUP:
                case type_kind_type::MESSAGE: {
                    if(auto nested_cols = col->nested()) {
                        auto nested_values = cursor_to_native_values(response, nested_cols->columns());
                        result.insert(result.end(), nested_values.begin(), nested_values.end());
                    } else {
                        result.emplace_back();
                    }
                    break;
                }

                default: result.emplace_back(); break;
            }
        }
    }
    return result;
}

void print_native_values(const std::vector<NativeValue>& values) {
    for(const auto& nv: values) {
        if(! nv.value()) {
            std::cout << "null";
        } else {
            std::visit(
                [](auto&& arg) {
                    using T = std::decay_t<decltype(arg)>;
                    if constexpr(std::is_same_v<T, std::monostate>) {
                        std::cout << "null";
                    } else if constexpr(std::is_same_v<T, bool>) {
                        std::cout << (arg ? "true" : "false");
                    } else {
                        std::cout << arg;
                    }
                },
                *nv.value()
            );
        }
        std::cout << " ";
    }
    std::cout << std::endl;
}
void print_columns(const std::vector<column_descriptor*>& cols, int indent = 0) {
    std::string indent_str(indent, ' ');

    for(const auto* col: cols) {
        std::cout << indent_str << "- column_name: " << col->column_name() << std::endl;
        std::cout << indent_str << "  type_kind: " << plugin::udf::to_string(col->type_kind()) << std::endl;

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
                std::cout << "            function_kind: " << plugin::udf::to_string(fn->function_kind()) << std::endl;

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
