/*
 * Copyright 2018-2024 Project Tsurugi.
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
#include "proto_debug_string.h"

#include <google/protobuf/stubs/status.h>
#include <google/protobuf/util/json_util.h>

namespace jogasaki::utils {

std::string to_debug_string(google::protobuf::Message const& message) {
    std::string ret{};
    google::protobuf::util::JsonPrintOptions options{};
    options.preserve_proto_field_names = true;
    if(auto st = MessageToJsonString(message, &ret, options); ! st.ok()) {
        return {};
    }
    return ret;
}

}

