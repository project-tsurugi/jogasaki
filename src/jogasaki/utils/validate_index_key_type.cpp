/*
 * Copyright 2018-2023 Project Tsurugi.
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
#include "validate_index_key_type.h"

#include <cstdint>

#include <takatori/type/octet.h>
#include <takatori/type/type_kind.h>
#include <takatori/util/downcast.h>
#include <takatori/util/string_builder.h>

#include <jogasaki/status.h>
#include <jogasaki/error/error_info_factory.h>

namespace jogasaki::utils {

using takatori::util::string_builder;
using takatori::util::unsafe_downcast;

bool validate_index_key_type(
    request_context& context,
    yugawara::storage::index const& i
) {
    using takatori::type::type_kind;
    for(auto&& k : i.keys()) {
        if(k.column().type().kind() == type_kind::octet) {
            if(auto&& typ = unsafe_downcast<takatori::type::octet const>(k.column().type()); typ.varying()) {
                set_error(
                    context,
                    error_code::unsupported_runtime_feature_exception,
                    string_builder{} << "data type used for column \"" << k.column().simple_name()
                                    << "\" is unsupported for primary/secondary index key" << string_builder::to_string,
                    status::err_unsupported
                );
                return false;
            }
        }
    }
    return true;
}

}  // namespace jogasaki::utils
