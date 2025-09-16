
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
namespace plugin::udf {
enum class function_kind_type {
    Unary,
    ClientStreaming,
    ServerStreaming,
    BidirectionalStreaming,
};
// @see
// https://github.com/protocolbuffers/protobuf/blob/main/src/google/protobuf/descriptor.proto#L243
enum class type_kind_type {
    FLOAT8,
    FLOAT4,
    INT8,
    UINT8,
    INT4,
    FIXED8,
    FIXED4,
    BOOL,
    STRING,
    GROUP,
    MESSAGE,
    BYTES,
    UINT4,
    ENUM,
    SFIXED4,
    SFIXED8,
    SINT4,
    SINT8,
};

enum class load_status {
    OK,
    PathNotFound,
    NotRegularFileOrDir,
    NoSharedObjectsFound,
    DLOpenFailed,
    ApiSymbolMissing,
    ApiInitFailed,
    FactorySymbolMissing,
    FactoryCreationFailed
};
} // namespace plugin::udf