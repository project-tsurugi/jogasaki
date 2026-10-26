/*
 * Copyright 2018-2026 Project Tsurugi.
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
#include "remote_catalog_encoder.h"

#include <exception>
#include <string_view>
#include <vector>

#include <takatori/relation/sort_direction.h>
#include <takatori/type/character.h>
#include <takatori/type/decimal.h>
#include <takatori/type/octet.h>
#include <takatori/type/row_id.h>
#include <takatori/type/table.h>
#include <takatori/type/time_of_day.h>
#include <takatori/type/time_point.h>
#include <takatori/type/type_kind.h>

#include <jogasaki/dist/remote_catalog_information.h>

namespace jogasaki::dist {

namespace {

namespace wire = ::tateyama::grpc::proto;

} // namespace

[[nodiscard]] static status encode_type(takatori::type::data const& type, wire::RemoteDataType& out,
                                        std::string& error) {
    namespace t = takatori::type;
    using k = t::type_kind;
    switch (type.kind()) {
        case k::boolean:
            out.mutable_boolean_type();
            return status::ok;
        case k::int1:
            out.mutable_int1_type();
            return status::ok;
        case k::int2:
            out.mutable_int2_type();
            return status::ok;
        case k::int4:
            out.mutable_int4_type();
            return status::ok;
        case k::int8:
            out.mutable_int8_type();
            return status::ok;
        case k::float4:
            out.mutable_float4_type();
            return status::ok;
        case k::float8:
            out.mutable_float8_type();
            return status::ok;
        case k::decimal: {
            auto const& source = static_cast<t::decimal const&>(type); // NOLINT
            auto* target = out.mutable_decimal_type();
            if (source.precision()) {
                target->set_precision(*source.precision());
            }
            if (source.scale()) {
                target->set_scale(*source.scale());
            }
            return status::ok;
        }
        case k::character: {
            auto const& source = static_cast<t::character const&>(type); // NOLINT
            auto* target = out.mutable_character_type();
            target->set_varying(source.varying());
            if (source.length()) {
                target->set_length(*source.length());
            }
            return status::ok;
        }
        case k::octet: {
            auto const& source = static_cast<t::octet const&>(type); // NOLINT
            auto* target = out.mutable_octet_type();
            target->set_varying(source.varying());
            if (source.length()) {
                target->set_length(*source.length());
            }
            return status::ok;
        }
        case k::date:
            out.mutable_date_type();
            return status::ok;
        case k::time_of_day:
            out.mutable_time_of_day_type()->set_with_time_zone(
                static_cast<t::time_of_day const&>(type).with_time_zone()); // NOLINT
            return status::ok;
        case k::time_point:
            out.mutable_time_point_type()->set_with_time_zone(
                static_cast<t::time_point const&>(type).with_time_zone()); // NOLINT
            return status::ok;
        case k::blob:
            out.mutable_blob_type();
            return status::ok;
        case k::clob:
            out.mutable_clob_type();
            return status::ok;
        case k::unknown:
            out.mutable_unknown_type();
            return status::ok;
        case k::row_id:
            out.mutable_row_id_type()->set_sequence(static_cast<t::row_id const&>(type).sequence()); // NOLINT
            return status::ok;
        case k::table: {
            auto* target = out.mutable_table_type();
            for (auto const& column : static_cast<t::table const&>(type).columns()) { // NOLINT
                auto* out_column = target->add_columns();
                out_column->set_name(std::string{column.name()});
                if (auto rc = encode_type(column.type(), *out_column->mutable_data_type(), error); rc != status::ok) {
                    return rc;
                }
            }
            return status::ok;
        }
        case k::bit:
        case k::datetime_interval:
        case k::array:
        case k::record:
        case k::row_reference:
        case k::declared:
        case k::extension:
            break;
    }
    error = "unsupported data type: " + std::string{t::to_string_view(type.kind())};
    return status::err_unsupported;
}

[[nodiscard]] static status encode_type(remote_catalog_information::type_ptr const& type, wire::RemoteDataType& out,
                                        std::string& error) {
    if (!type) {
        error = "missing data type";
        return status::err_unknown;
    }
    return encode_type(*type, out, error);
}

[[nodiscard]] static status encode_feature(yugawara::storage::index_feature feature, wire::RemoteIndex& out) {
    using yugawara::storage::index_feature;
    if (feature == index_feature::primary) {
        out.add_features(wire::REMOTE_INDEX_FEATURE_PRIMARY);
    } else if (feature == index_feature::find) {
        out.add_features(wire::REMOTE_INDEX_FEATURE_FIND);
    } else if (feature == index_feature::scan) {
        out.add_features(wire::REMOTE_INDEX_FEATURE_SCAN);
    } else if (feature == index_feature::unique) {
        out.add_features(wire::REMOTE_INDEX_FEATURE_UNIQUE);
    } else if (feature == index_feature::unique_constraint) {
        out.add_features(wire::REMOTE_INDEX_FEATURE_UNIQUE_CONSTRAINT);
    } else {
        return status::err_unsupported;
    }
    return status::ok;
}

[[nodiscard]] static status encode_feature(yugawara::function::function_feature feature, wire::RemoteFunction& out) {
    using yugawara::function::function_feature;
    if (feature == function_feature::scalar_function) {
        out.add_features(wire::REMOTE_FUNCTION_FEATURE_SCALAR);
    } else if (feature == function_feature::table_valued_function) {
        out.add_features(wire::REMOTE_FUNCTION_FEATURE_TABLE_VALUED);
    } else {
        return status::err_unsupported;
    }
    return status::ok;
}

template <class Source, class Target>
[[nodiscard]] static status encode_signature(Source const& source, Target& target, std::string& error) {
    target.set_remote_definition_id(source.remote_definition_id);
    target.set_function_name(source.name);
    for (auto const& parameter : source.parameter_types) {
        if (auto rc = encode_type(parameter, *target.add_parameter_types(), error); rc != status::ok) {
            error += " in parameters of " + source.name;
            return rc;
        }
    }
    if (auto rc = encode_type(source.return_type, *target.mutable_return_type(), error); rc != status::ok) {
        error += " in return type of " + source.name;
        return rc;
    }
    return status::ok;
}

[[nodiscard]] static status encode_schemas(remote_catalog_information const& plan, wire::RemoteCatalog& out,
                                           std::string&) {
    for (auto const& source : plan.schemas) {
        auto* target = out.add_schema_list();
        target->set_schema_name(source.name);
        for (auto i : source.tables) {
            target->add_table_index_list(i);
        }
        for (auto i : source.indices) {
            target->add_index_index_list(i);
        }
        for (auto i : source.functions) {
            target->add_function_index_list(i);
        }
        for (auto i : source.aggregate_functions) {
            target->add_aggregate_function_index_list(i);
        }
    }
    return status::ok;
}

[[nodiscard]] static status encode_tables(remote_catalog_information const& plan, wire::RemoteCatalog& out,
                                          std::string&) {
    for (auto const& source : plan.tables) {
        auto* target = out.add_table_list();
        if (source.remote_definition_id) {
            target->set_remote_definition_id(*source.remote_definition_id);
        }
        target->set_table_name(source.name);
        for (auto i : source.columns) {
            target->add_column_index_list(i);
        }
    }
    return status::ok;
}

[[nodiscard]] static status encode_columns(remote_catalog_information const& plan, wire::RemoteCatalog& out,
                                           std::string& error) {
    for (auto const& source : plan.columns) {
        auto* target = out.add_column_list();
        target->set_column_name(source.name);
        if (auto rc = encode_type(source.type, *target->mutable_data_type(), error); rc != status::ok) {
            error += " in column " + source.name;
            return rc;
        }
        target->set_nullable(source.nullable);
    }
    return status::ok;
}

[[nodiscard]] static status encode_indices(remote_catalog_information const& plan, wire::RemoteCatalog& out,
                                           std::string& error) {
    for (auto const& source : plan.indices) {
        auto* target = out.add_index_list();
        if (source.remote_definition_id) {
            target->set_remote_definition_id(*source.remote_definition_id);
        }
        target->set_index_name(source.name);
        if (source.table) {
            target->set_table_index(*source.table);
        }
        for (auto feature : source.features) {
            if (encode_feature(feature, *target) != status::ok) {
                error = "unsupported index feature in index " + source.name;
                return status::err_unsupported;
            }
        }
        for (auto const& key : source.keys) {
            auto* out_key = target->add_keys();
            if (key.column) {
                out_key->set_column_index(*key.column);
            }
            out_key->set_direction(key.direction == takatori::relation::sort_direction::descendant
                                       ? wire::REMOTE_SORT_DIRECTION_DESCENDING
                                       : wire::REMOTE_SORT_DIRECTION_ASCENDING);
        }
        for (auto i : source.values) {
            target->add_value_column_index_list(i);
        }
    }
    return status::ok;
}

[[nodiscard]] static status encode_functions(remote_catalog_information const& plan, wire::RemoteCatalog& out,
                                             std::string& error) {
    for (auto const& source : plan.functions) {
        auto* target = out.add_function_list();
        if (auto rc = encode_signature(source, *target, error); rc != status::ok) {
            return rc;
        }
        for (auto feature : source.features) {
            if (encode_feature(feature, *target) != status::ok) {
                error = "unsupported function feature in function " + source.name;
                return status::err_unsupported;
            }
        }
    }
    return status::ok;
}

[[nodiscard]] static status encode_aggregate_functions(remote_catalog_information const& plan, wire::RemoteCatalog& out,
                                                       std::string& error) {
    for (auto const& source : plan.aggregate_functions) {
        auto* target = out.add_aggregate_function_list();
        if (auto rc = encode_signature(source, *target, error); rc != status::ok) {
            return rc;
        }
        target->set_incremental(source.incremental);
    }
    return status::ok;
}

namespace {

/**
 * @brief message encoding of a complete plan.
 * @details Elements are appended in plan order and every relation uses the transfer index fixed by the plan.
 */
} // namespace

[[nodiscard]] static status encode_message(remote_catalog_information const& plan, wire::RemoteCatalog& out,
                                           std::string& error) {
    if (auto rc = encode_schemas(plan, out, error); rc != status::ok) {
        return rc;
    }
    if (auto rc = encode_tables(plan, out, error); rc != status::ok) {
        return rc;
    }
    if (auto rc = encode_columns(plan, out, error); rc != status::ok) {
        return rc;
    }
    if (auto rc = encode_indices(plan, out, error); rc != status::ok) {
        return rc;
    }
    if (auto rc = encode_functions(plan, out, error); rc != status::ok) {
        return rc;
    }
    if (auto rc = encode_aggregate_functions(plan, out, error); rc != status::ok) {
        return rc;
    }
    return status::ok;
}

status remote_catalog_encoder::encode(::yugawara::schema::catalog const& source,
                                      wire::RemoteCatalog& destination) const {
    std::string diagnostic{};
    return encode(source, destination, diagnostic);
}

status remote_catalog_encoder::encode(::yugawara::schema::catalog const& source, wire::RemoteCatalog& destination,
                                      std::string& diagnostic) const {
    // both the plan and the message are temporaries of this call
    remote_catalog_information plan{};
    if (auto rc = describe_remote_catalog(source, plan, diagnostic); rc != status::ok) {
        return rc;
    }
    wire::RemoteCatalog message{};
    std::string error{};
    status rc = status::ok;
    try {
        rc = encode_message(plan, message, error);
    } catch (std::exception const& e) {
        error = e.what();
        rc = status::err_unknown;
    }
    if (rc != status::ok) {
        diagnostic = "cannot encode remote catalog: " + error;
        return rc;
    }
    destination.Swap(&message);
    return status::ok;
}

} // namespace jogasaki::dist
