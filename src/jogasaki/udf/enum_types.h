
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
    unary,
    client_streaming,
    server_streaming,
    bidirectional_streaming,
};
// @see
// https://github.com/protocolbuffers/protobuf/blob/main/src/google/protobuf/descriptor.proto#L243
enum class type_kind_type {
    float8,
    float4,
    int8,
    uint8,
    int4,
    fixed8,
    fixed4,
    boolean,
    string,
    group,
    message,
    bytes,
    uint4,
    grpc_enum,
    sfixed4,
    sfixed8,
    sint4,
    sint8,
};

enum class load_status {
    ok,
    path_not_found,
    no_ini_and_so_files,
    ini_so_pair_mismatch,
    ini_invalid,
    not_regular_file_or_dir,
    udf_disabled,
    no_shared_objects_found,
    dlopen_failed,
    api_symbol_missing,
    api_init_failed,
    factory_symbol_missing,
    factory_creation_failed
};
}  // namespace plugin::udf