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
#include "validate_table_definition.h"

#include <string_view>

#include <takatori/type/character.h>
#include <takatori/type/decimal.h>
#include <takatori/type/octet.h>
#include <takatori/type/type_kind.h>
#include <takatori/util/downcast.h>
#include <takatori/util/string_builder.h>
#include <yugawara/storage/column.h>
#include <yugawara/storage/column_value.h>

#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/executor/conv/create_default_value.h>
#include <jogasaki/status.h>

namespace jogasaki::utils {

using takatori::util::string_builder;
using takatori::util::unsafe_downcast;

bool validate_type(
    request_context& context,
    std::string_view colname,
    takatori::type::decimal const& typ
) {
    std::string_view reason{};
    if(! typ.scale()) {
        reason = "invalid scale";
    } else if(typ.precision() && ! (typ.precision().value() >= decimal_min_precision && typ.precision().value() <= decimal_max_precision)) {
        reason = "invalid precision";
    } else if(typ.precision() && typ.scale() && ! (typ.scale().value() <= typ.precision().value())) {
        reason = "scale out of range for the precision";
    } else {
        return true;
    }
    set_error(
        context,
        error_code::unsupported_runtime_feature_exception,
        string_builder{} << "decimal type on column \"" << colname << "\" is unsupported (" << reason << ")" << string_builder::to_string,
        status::err_unsupported
    );
    return false;
}

bool validate_type(
    request_context& context,
    std::string_view colname,
    takatori::type::character const& typ
) {
    std::string_view reason{};
    if(typ.length() && !(typ.length().value() >= 1 && typ.length().value() <= character_type_max_length_for_value)) {
        reason = "invalid length";
    } else {
        return true;
    }
    set_error(
        context,
        error_code::unsupported_runtime_feature_exception,
        string_builder{} << "character type on column \"" << colname << "\" is unsupported (" << reason << ")" << string_builder::to_string,
        status::err_unsupported
    );
    return false;
}

bool validate_type(
    request_context& context,
    std::string_view colname,
    takatori::type::octet const& typ
) {
    std::string_view reason{};
    if(typ.length() && !(typ.length().value() >= 1 && typ.length().value() <= octet_type_max_length_for_value)) {
        reason = "invalid length";
    } else {
        return true;
    }
    set_error(
        context,
        error_code::unsupported_runtime_feature_exception,
        string_builder{} << "octet type on column \"" << colname << "\" is unsupported (" << reason << ")" << string_builder::to_string,
        status::err_unsupported
    );
    return false;
}

bool validate_default_value(
    request_context& context,
    yugawara::storage::column const& c
) {
    if(c.default_value().kind() == yugawara::storage::column_value_kind::immediate) {
        auto& dv = c.default_value().element<yugawara::storage::column_value_kind::immediate>();
        if(auto a = executor::conv::create_immediate_default_value(*dv, c.type(), context.request_resource()); a.error()) {
            set_error(
                context,
                error_code::unsupported_runtime_feature_exception,
                string_builder{} << "unable to convert default value for column \"" << c.simple_name()
                                 << "\" to type " << c.type() << string_builder::to_string,
                status::err_unsupported
            );
            return false;
        }
    }
    return true;
}

bool validate_table_definition(
    request_context& context,
    yugawara::storage::table const& t
) {
    using takatori::type::type_kind;
    for(auto&& c : t.columns()) {
        if(! validate_default_value(context, c)) {
            return false;
        }
        switch(c.type().kind()) {
            case type_kind::decimal:
                if(! validate_type(context, c.simple_name(), unsafe_downcast<takatori::type::decimal const>(c.type()))) {
                    return false;
                }
                continue;
            case type_kind::character:
                if(! validate_type(context, c.simple_name(), unsafe_downcast<takatori::type::character const>(c.type()))) {
                    return false;
                }
                continue;
            case type_kind::int4:
            case type_kind::int8:
            case type_kind::float4:
            case type_kind::float8:
            case type_kind::date:
            case type_kind::time_of_day:
            case type_kind::time_point:
                continue;
            case type_kind::octet:
                if(! validate_type(context, c.simple_name(), unsafe_downcast<takatori::type::octet const>(c.type()))) {
                    return false;
                }
                continue;
            case type_kind::boolean:
                if(context.configuration()->support_boolean()) {
                    continue;
                }
                break;
            case type_kind::int1:
            case type_kind::int2:
                if(context.configuration()->support_smallint()) {
                    continue;
                }
                break;
            default:
                break;
        }
        set_error(
            context,
            error_code::unsupported_runtime_feature_exception,
            string_builder{} << "Data type specified for column \"" << c.simple_name() << "\" is unsupported."
                             << string_builder::to_string,
            status::err_unsupported
        );
        return false;
    }
    return true;
}

}  // namespace jogasaki::utils
