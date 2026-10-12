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
#include "remote_catalog_decoder.h"

#include <exception>
#include <memory>
#include <optional>
#include <set>
#include <utility>
#include <vector>
#include <google/protobuf/unknown_field_set.h>

#include <takatori/relation/sort_direction.h>
#include <takatori/type/blob.h>
#include <takatori/type/character.h>
#include <takatori/type/clob.h>
#include <takatori/type/date.h>
#include <takatori/type/decimal.h>
#include <takatori/type/octet.h>
#include <takatori/type/primitive.h>
#include <takatori/type/row_id.h>
#include <takatori/type/table.h>
#include <takatori/type/time_of_day.h>
#include <takatori/type/time_point.h>
#include <takatori/type/varying.h>
#include <takatori/type/with_time_zone.h>

namespace jogasaki::dist {

namespace {

namespace wire = ::tateyama::grpc::proto;
using index_type = remote_catalog_information::index_type;
using type_ptr = remote_catalog_information::type_ptr;

class decoding {
public:
    [[nodiscard]] bool catalog(wire::RemoteCatalog const& source, remote_catalog_information& out) {
        return decode_schemas(source, out) &&
            decode_tables(source, out) &&
            decode_columns(source, out) &&
            decode_indices(source, out) &&
            decode_functions(source, out) &&
            decode_aggregate_functions(source, out);
    }

    [[nodiscard]] status result() const noexcept { return result_; }
    [[nodiscard]] std::string const& error() const noexcept { return error_; }

    bool fail(status rc, std::string message) {
        result_ = rc;
        error_ = std::move(message);
        return false;
    }

private:
    [[nodiscard]] bool decode_schemas(wire::RemoteCatalog const& source, remote_catalog_information& out) {
        for (auto const& s : source.schema_list()) {
            remote_catalog_information::schema schema{s.schema_name(), {}, {}, {}, {}};
            if (!indices(s.table_index_list(), source.table_list_size(), schema.tables, "schema table") ||
                !indices(s.index_index_list(), source.index_list_size(), schema.indices, "schema index") ||
                !indices(s.function_index_list(), source.function_list_size(), schema.functions, "schema function") ||
                !indices(s.aggregate_function_index_list(), source.aggregate_function_list_size(),
                    schema.aggregate_functions, "schema aggregate function")) {
                return false;
            }
            out.schemas.push_back(std::move(schema));
        }
        return true;
    }

    [[nodiscard]] bool decode_tables(wire::RemoteCatalog const& source, remote_catalog_information& out) {
        for (auto const& t : source.table_list()) {
            remote_catalog_information::table table{t.remote_definition_id(), t.table_name(), {}};
            if (!indices(t.column_index_list(), source.column_list_size(), table.columns, "table column")) return false;
            out.tables.push_back(std::move(table));
        }
        return true;
    }

    [[nodiscard]] bool decode_columns(wire::RemoteCatalog const& source, remote_catalog_information& out) {
        for (auto const& c : source.column_list()) {
            if (!c.has_data_type()) return corrupt("column has no data type: " + c.column_name());
            remote_catalog_information::column column{c.column_name(), {}, c.nullable()};
            if (!type(c.data_type(), column.type)) return context("column " + c.column_name());
            out.columns.push_back(std::move(column));
        }
        return true;
    }

    [[nodiscard]] bool decode_indices(wire::RemoteCatalog const& source, remote_catalog_information& out) {
        for (auto const& i : source.index_list()) {
            remote_catalog_information::index index{i.remote_definition_id(), i.index_name(), {}, {}, {}, {}};
            // an absent index stays absent; it is not the first element
            if (i.table_index_presence_case() == wire::RemoteIndex::kTableIndex) {
                if (!in_range(i.table_index(), source.table_list_size(), "index table")) return false;
                index.table = i.table_index();
            }
            if (!index_features(i, index.features)) return false;
            for (auto const& k : i.keys()) {
                remote_catalog_information::index_key key{};
                if (k.column_index_presence_case() == wire::RemoteIndexKey::kColumnIndex) {
                    if (!in_range(k.column_index(), source.column_list_size(), "index key column")) return false;
                    key.column = k.column_index();
                }
                if (!direction(k.direction(), key.direction)) return context("index " + i.index_name());
                index.keys.push_back(key);
            }
            if (!indices(i.value_column_index_list(), source.column_list_size(), index.values, "index value column")) {
                return false;
            }
            out.indices.push_back(std::move(index));
        }
        return true;
    }

    [[nodiscard]] bool decode_functions(wire::RemoteCatalog const& source, remote_catalog_information& out) {
        for (auto const& f : source.function_list()) {
            remote_catalog_information::function function{f.remote_definition_id(), f.function_name(), {}, {}, {}};
            if (!signature(f, function.parameter_types, function.return_type) ||
                !function_features(f, function.features)) {
                return context("function " + f.function_name());
            }
            out.functions.push_back(std::move(function));
        }
        return true;
    }

    [[nodiscard]] bool decode_aggregate_functions(wire::RemoteCatalog const& source, remote_catalog_information& out) {
        for (auto const& f : source.aggregate_function_list()) {
            remote_catalog_information::aggregate_function function{
                f.remote_definition_id(), f.function_name(), {}, {}, f.incremental()};
            if (!signature(f, function.parameter_types, function.return_type)) {
                return context("aggregate function " + f.function_name());
            }
            out.aggregate_functions.push_back(std::move(function));
        }
        return true;
    }

    status result_{status::ok};
    std::string error_{};

    bool corrupt(std::string message) { return fail(status::err_data_corruption, std::move(message)); }
    bool unsupported(std::string message) { return fail(status::err_unsupported, std::move(message)); }
    bool context(std::string const& where) {
        error_ += " in " + where;
        return false;
    }

    [[nodiscard]] bool in_range(std::uint64_t index, int size, char const* what) {
        if (index >= static_cast<std::uint64_t>(size)) {
            return corrupt(std::string{what} + " index out of range: " + std::to_string(index));
        }
        return true;
    }

    template <class Field>
    [[nodiscard]] bool indices(Field const& field, int size, std::vector<index_type>& out, char const* what) {
        for (auto index : field) {
            if (!in_range(index, size, what)) return false;
            out.push_back(index);
        }
        return true;
    }

    [[nodiscard]] bool type(wire::RemoteDataType const& in, type_ptr& out) {
        namespace t = takatori::type;
        using value = wire::RemoteDataType;
        auto optional_size = [](bool present, std::uint64_t v) -> std::optional<std::size_t> {
            if (!present) return std::nullopt;
            return static_cast<std::size_t>(v);
        };
        switch (in.value_case()) {
            case value::kBooleanType: out = std::make_shared<t::boolean>(); return true;
            case value::kInt1Type: out = std::make_shared<t::int1>(); return true;
            case value::kInt2Type: out = std::make_shared<t::int2>(); return true;
            case value::kInt4Type: out = std::make_shared<t::int4>(); return true;
            case value::kInt8Type: out = std::make_shared<t::int8>(); return true;
            case value::kFloat4Type: out = std::make_shared<t::float4>(); return true;
            case value::kFloat8Type: out = std::make_shared<t::float8>(); return true;
            case value::kDecimalType: {
                auto const& d = in.decimal_type();
                bool const has_precision = d.precision_presence_case() == wire::RemoteDecimalType::kPrecision;
                bool const has_scale = d.scale_presence_case() == wire::RemoteDecimalType::kScale;
                if (has_precision && d.precision() == 0) return corrupt("decimal precision is 0");
                if (has_precision && has_scale && d.scale() > d.precision()) {
                    return corrupt("decimal scale exceeds precision");
                }
                out = std::make_shared<t::decimal>(
                    optional_size(has_precision, d.precision()), optional_size(has_scale, d.scale()));
                return true;
            }
            case value::kCharacterType: {
                auto const& c = in.character_type();
                out = std::make_shared<t::character>(t::varying_t{c.varying()}, optional_size(c.length_presence_case() == wire::RemoteVaryingLengthType::kLength, c.length()));
                return true;
            }
            case value::kOctetType: {
                auto const& o = in.octet_type();
                out = std::make_shared<t::octet>(t::varying_t{o.varying()}, optional_size(o.length_presence_case() == wire::RemoteVaryingLengthType::kLength, o.length()));
                return true;
            }
            case value::kBitType: return unsupported("BIT is not supported in remote execution");
            case value::kDateType: out = std::make_shared<t::date>(); return true;
            case value::kTimeOfDayType:
                out = std::make_shared<t::time_of_day>(t::with_time_zone_t{in.time_of_day_type().with_time_zone()});
                return true;
            case value::kTimePointType:
                out = std::make_shared<t::time_point>(t::with_time_zone_t{in.time_point_type().with_time_zone()});
                return true;
            case value::kDatetimeIntervalType: return unsupported("INTERVAL is not supported in remote execution");
            case value::kBlobType: out = std::make_shared<t::blob>(); return true;
            case value::kClobType: out = std::make_shared<t::clob>(); return true;
            case value::kUnknownType: out = std::make_shared<t::unknown>(); return true;
            case value::kRowIdType:
                out = std::make_shared<t::row_id>(static_cast<std::size_t>(in.row_id_type().sequence()));
                return true;
            case value::kTableType: {
                std::vector<t::table::column_type> columns{};
                for (auto const& c : in.table_type().columns()) {
                    if (!c.has_data_type()) return corrupt("table type column has no data type: " + c.name());
                    type_ptr column_type{};
                    if (!type(c.data_type(), column_type)) return context("table type column " + c.name());
                    columns.emplace_back(c.name(), std::move(column_type));
                }
                out = std::make_shared<t::table>(std::move(columns));
                return true;
            }
            case value::VALUE_NOT_SET:
                break;
        }
        // a case added by a later minor version arrives as an unknown field
        if (wire::RemoteDataType::GetReflection()->GetUnknownFields(in).field_count() != 0) return unsupported("unknown RemoteDataType case");
        return corrupt("RemoteDataType has no value");
    }

    template <class Source>
    [[nodiscard]] bool signature(Source const& source, std::vector<type_ptr>& parameters, type_ptr& return_type) {
        for (auto const& p : source.parameter_types()) {
            type_ptr parameter{};
            if (!type(p, parameter)) return false;
            parameters.push_back(std::move(parameter));
        }
        if (!source.has_return_type()) return corrupt("missing return type");
        return type(source.return_type(), return_type);
    }

    [[nodiscard]] bool direction(int value, takatori::relation::sort_direction& out) {
        if (!wire::RemoteSortDirection_IsValid(value)) return unsupported("unknown sort direction " + std::to_string(value));
        switch (static_cast<wire::RemoteSortDirection>(value)) {
            case wire::REMOTE_SORT_DIRECTION_ASCENDING: out = takatori::relation::sort_direction::ascendant; return true;
            case wire::REMOTE_SORT_DIRECTION_DESCENDING: out = takatori::relation::sort_direction::descendant; return true;
            default: break;
        }
        return corrupt("unspecified sort direction");
    }

    // shared rules of feature enums: unknown is UNIMPLEMENTED, unspecified or duplicate is malformed
    template <class Field, class IsValid>
    [[nodiscard]] bool feature_values(Field const& field, IsValid is_valid, std::vector<int>& out, char const* what) {
        std::set<int> seen{};
        for (int value : field) {
            if (!is_valid(value)) return unsupported(std::string{"unknown "} + what + " " + std::to_string(value));
            if (value == 0) return corrupt(std::string{"unspecified "} + what);
            if (!seen.insert(value).second) return corrupt(std::string{"duplicate "} + what + " " + std::to_string(value));
            out.push_back(value);
        }
        return true;
    }

    [[nodiscard]] bool index_features(wire::RemoteIndex const& source, yugawara::storage::index_feature_set& out) {
        using yugawara::storage::index_feature;
        std::vector<int> values{};
        if (!feature_values(source.features(), wire::RemoteIndexFeature_IsValid, values, "index feature")) {
            return context("index " + source.index_name());
        }
        for (auto value : values) {
            switch (static_cast<wire::RemoteIndexFeature>(value)) {
                case wire::REMOTE_INDEX_FEATURE_PRIMARY: out.insert(index_feature::primary); break;
                case wire::REMOTE_INDEX_FEATURE_FIND: out.insert(index_feature::find); break;
                case wire::REMOTE_INDEX_FEATURE_SCAN: out.insert(index_feature::scan); break;
                case wire::REMOTE_INDEX_FEATURE_UNIQUE: out.insert(index_feature::unique); break;
                case wire::REMOTE_INDEX_FEATURE_UNIQUE_CONSTRAINT: out.insert(index_feature::unique_constraint); break;
                default: return unsupported("unhandled index feature " + std::to_string(value));
            }
        }
        return true;
    }

    [[nodiscard]] bool function_features(wire::RemoteFunction const& source, yugawara::function::function_feature_set& out) {
        using yugawara::function::function_feature;
        std::vector<int> values{};
        if (!feature_values(source.features(), wire::RemoteFunctionFeature_IsValid, values, "function feature")) {
            return false;
        }
        for (auto value : values) {
            switch (static_cast<wire::RemoteFunctionFeature>(value)) {
                case wire::REMOTE_FUNCTION_FEATURE_SCALAR: out.insert(function_feature::scalar_function); break;
                case wire::REMOTE_FUNCTION_FEATURE_TABLE_VALUED: out.insert(function_feature::table_valued_function); break;
                default: return unsupported("unhandled function feature " + std::to_string(value));
            }
        }
        return true;
    }
};

}  // namespace

status remote_catalog_decoder::decode(
    wire::RemoteCatalog const& source,
    remote_catalog_information& destination,
    std::string& diagnostic
) const {
    decoding state{};
    remote_catalog_information result{};
    try {
        if (!state.catalog(source, result)) {
            diagnostic = "invalid RemoteCatalog: " + state.error();
            return state.result();
        }
    } catch (std::exception const& e) {
        // Takatori rejects invalid type parameters by throwing
        diagnostic = std::string{"invalid RemoteCatalog: "} + e.what();
        return status::err_data_corruption;
    }
    destination = std::move(result);
    return status::ok;
}

status remote_catalog_fragment_assembler::fail(status rc, std::string message, std::string& diagnostic) {
    reset();
    failed_ = true;
    diagnostic = std::move(message);
    return rc;
}

status remote_catalog_fragment_assembler::accept(wire::RemoteCatalogResponse& response, std::string& diagnostic) {
    if (failed_) {
        diagnostic = "remote catalog stream already failed";
        return status::err_invalid_state;
    }
    auto size = response.ByteSizeLong();
    if (size > remote_catalog_max_message_size) {
        return fail(status::err_resource_limit_reached, "remote catalog response exceeds 64 MiB", diagnostic);
    }
    if (response.sequence_number() != next_) {
        return fail(status::err_data_corruption, "remote catalog response sequence " +
            std::to_string(response.sequence_number()) + " where " + std::to_string(next_) + " was expected", diagnostic);
    }
    if (!response.has_catalog_fragment()) {
        return fail(status::err_data_corruption, "remote catalog response has no fragment", diagnostic);
    }
    auto& f = *response.mutable_catalog_fragment();
    if (f.schema_list_size() + f.table_list_size() + f.column_list_size() + f.index_list_size() +
            f.function_list_size() + f.aggregate_function_list_size() == 0) {
        return fail(status::err_data_corruption, "remote catalog response has an empty fragment", diagnostic);
    }
    // fragments are concatenated field by field in sequence order
    for (auto& e : *f.mutable_schema_list()) catalog_.add_schema_list()->Swap(&e);
    for (auto& e : *f.mutable_table_list()) catalog_.add_table_list()->Swap(&e);
    for (auto& e : *f.mutable_column_list()) catalog_.add_column_list()->Swap(&e);
    for (auto& e : *f.mutable_index_list()) catalog_.add_index_list()->Swap(&e);
    for (auto& e : *f.mutable_function_list()) catalog_.add_function_list()->Swap(&e);
    for (auto& e : *f.mutable_aggregate_function_list()) catalog_.add_aggregate_function_list()->Swap(&e);
    ++next_;
    bytes_ += size;
    return status::ok;
}

status remote_catalog_fragment_assembler::finish(remote_catalog_information& destination, std::string& diagnostic) {
    if (failed_) {
        diagnostic = "remote catalog stream already failed";
        reset();
        return status::err_invalid_state;
    }
    auto rc = remote_catalog_decoder{}.decode(catalog_, destination, diagnostic);
    reset();
    return rc;
}

void remote_catalog_fragment_assembler::reset() noexcept {
    catalog_.Clear();
    next_ = 0;
    bytes_ = 0;
    failed_ = false;
}

std::uint64_t remote_catalog_fragment_assembler::fragments() const noexcept {
    return next_;
}

std::size_t remote_catalog_fragment_assembler::bytes() const noexcept {
    return bytes_;
}

}  // namespace jogasaki::dist
