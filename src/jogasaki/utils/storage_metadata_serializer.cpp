/*
 * Copyright 2018-2020 tsurugi project.
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
#include "storage_metadata_serializer.h"

#include <initializer_list>

#include <takatori/util/fail.h>
#include <takatori/type/boolean.h>
#include <takatori/type/int.h>
#include <takatori/type/float.h>
#include <takatori/type/character.h>
#include <takatori/type/octet.h>
#include <takatori/type/decimal.h>
#include <takatori/type/date.h>
#include <takatori/type/time_of_day.h>
#include <takatori/type/time_point.h>
#include <takatori/value/boolean.h>
#include <takatori/value/int.h>
#include <takatori/value/float.h>
#include <takatori/value/character.h>
#include <takatori/value/octet.h>
#include <takatori/value/decimal.h>
#include <takatori/value/date.h>
#include <takatori/value/time_of_day.h>
#include <takatori/value/time_point.h>
#include <takatori/datetime/date.h>
#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/time_point.h>

#include <yugawara/variable/criteria.h>

#include <jogasaki/logging.h>
#include <jogasaki/data/aligned_buffer.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/data/any.h>
#include <jogasaki/utils/decimal.h>

#include <jogasaki/proto/metadata/storage.pb.h>

namespace jogasaki::utils {

using takatori::util::fail;

storage_metadata_serializer::storage_metadata_serializer() noexcept = default;

namespace details {

proto::metadata::common::AtomType from(takatori::type::data const& t) {
    using proto::metadata::common::AtomType;
    using k = takatori::type::type_kind;
    switch(t.kind()) {
        case k::boolean: return AtomType::BOOLEAN;
        case k::int1: return AtomType::INT1;
        case k::int2: return AtomType::INT2;
        case k::int4: return AtomType::INT4;
        case k::int8: return AtomType::INT8;
        case k::float4: return AtomType::FLOAT4;
        case k::float8: return AtomType::FLOAT8;
        case k::decimal: return AtomType::DECIMAL;
        case k::character: return AtomType::CHARACTER;
        case k::octet: return AtomType::OCTET;
        case k::bit: return AtomType::BIT;
        case k::date: return AtomType::DATE;
        case k::time_of_day: return AtomType::TIME_OF_DAY;  // possibly updated 'with time zone'
        case k::time_point: return AtomType::TIME_POINT;  // possibly updated 'with time zone'
        case k::datetime_interval: return AtomType::DATETIME_INTERVAL;
        case k::unknown: return AtomType::UNKNOWN;
        default: return AtomType::TYPE_UNSPECIFIED;
    }
    fail();
}

void set_type(::jogasaki::proto::metadata::storage::TableColumn* col, yugawara::storage::column const& c) {
    auto typ = col->mutable_type();
    typ->set_atom_type(from(c.type()));
    switch(c.type().kind()) {
        using proto::metadata::common::AtomType;
        using k = takatori::type::type_kind;
        case k::decimal: {
            auto& d = static_cast<takatori::type::decimal const&>(c.type());  //NOLINT
            auto* opt = typ->mutable_decimal_option();
            if(d.precision().has_value()) {
                opt->set_precision(static_cast<std::int64_t>(*d.precision()));
            }
            if(d.scale().has_value()) {
                opt->set_scale(static_cast<std::int64_t>(*d.scale()));
            }
            break;
        }
        case k::character: {
            auto& d = static_cast<takatori::type::character const&>(c.type());  //NOLINT
            auto* op = typ->mutable_character_option();
            op->set_varying(d.varying());
            if(d.length().has_value()) {
                op->set_length(static_cast<std::int64_t>(*d.length()));
            }
            break;
        }
        case k::octet: {
            auto& d = static_cast<takatori::type::octet const&>(c.type());  //NOLINT
            auto* op = typ->mutable_octet_option();
            op->set_varying(d.varying());
            if(d.length().has_value()) {
                op->set_length(static_cast<std::int64_t>(*d.length()));
            }
            break;
        }
        case k::time_of_day: {
            auto& d = static_cast<takatori::type::time_of_day const&>(c.type());  //NOLINT
            typ->set_atom_type(d.with_time_zone() ? AtomType::TIME_OF_DAY_WITH_TIME_ZONE : AtomType::TIME_OF_DAY);
            break;
        }
        case k::time_point: {
            auto& d = static_cast<takatori::type::time_point const&>(c.type());  //NOLINT
            typ->set_atom_type(d.with_time_zone() ? AtomType::TIME_POINT_WITH_TIME_ZONE : AtomType::TIME_POINT);
            break;
        }
        default: break;
    }
}

void set_default(::jogasaki::proto::metadata::storage::TableColumn* col, yugawara::storage::column const& c) {
    switch(c.default_value().kind()) {
        case yugawara::storage::column_value_kind::nothing: {
            col->clear_default_value();
            break;
        }
        case yugawara::storage::column_value_kind::immediate: {
            auto& value = c.default_value().element<yugawara::storage::column_value_kind::immediate>();
            switch(c.type().kind()) {
                using proto::metadata::common::AtomType;
                using k = takatori::type::type_kind;
                case k::boolean: col->set_boolean_value(static_cast<takatori::value::boolean const&>(*value).get()); break; //NOLINT
                case k::int1: col->set_int4_value(static_cast<takatori::value::int4 const&>(*value).get()); break;  //TODO verify cast correctness //NOLINT
                case k::int2: col->set_int4_value(static_cast<takatori::value::int4 const&>(*value).get()); break;  //TODO verify cast correctness //NOLINT
                case k::int4: col->set_int4_value(static_cast<takatori::value::int4 const&>(*value).get()); break; //NOLINT
                case k::int8: col->set_int8_value(static_cast<takatori::value::int8 const&>(*value).get()); break; //NOLINT
                case k::float4: col->set_float4_value(static_cast<takatori::value::float4 const&>(*value).get()); break; //NOLINT
                case k::float8: col->set_float8_value(static_cast<takatori::value::float8 const&>(*value).get()); break; //NOLINT
                case k::decimal: {
                    auto p = static_cast<takatori::value::decimal const&>(*value).get();  //NOLINT
                    utils::decimal_buffer out{};
                    auto [hi, lo, sz] = utils::make_signed_coefficient_full(p);
                    utils::create_decimal(p.sign(), lo, hi, sz, out);
                    auto v = col->mutable_decimal_value();
                    v->set_unscaled_value(out.data(), sz);
                    v->set_exponent(p.exponent());
                    break;
                }
                case k::character: col->set_character_value(std::string{static_cast<takatori::value::character const&>(*value).get()}); break; //NOLINT
                case k::octet: col->set_octet_value(std::string{static_cast<takatori::value::octet const&>(*value).get()}); break; //NOLINT
                case k::date: col->set_date_value(static_cast<takatori::value::date const&>(*value).get().days_since_epoch()); break; //NOLINT
                case k::time_of_day: {
                    auto p = static_cast<takatori::value::time_of_day const&>(*value).get();  //NOLINT
                    auto& d = static_cast<takatori::type::time_of_day const&>(c.type());  //NOLINT
                    if(d.with_time_zone()) {
                        auto v = col->mutable_time_of_day_with_time_zone_value();
                        v->set_time_zone_offset(0); // UTC for now
                        v->set_offset_nanoseconds(p.time_since_epoch().count());
                    } else {
                        col->set_time_of_day_value(p.time_since_epoch().count());
                    }
                    break;
                }
                case k::time_point: {
                    auto p = static_cast<takatori::value::time_point const&>(*value).get();  //NOLINT
                    auto& d = static_cast<takatori::type::time_point const&>(c.type());  //NOLINT
                    if(d.with_time_zone()) {
                        auto v = col->mutable_time_point_with_time_zone_value();
                        v->set_time_zone_offset(0); // UTC for now
                        v->set_offset_seconds(p.seconds_since_epoch().count());
                        v->set_nano_adjustment(p.subsecond().count());
                    } else {
                        auto v = col->mutable_time_point_value();
                        v->set_offset_seconds(p.seconds_since_epoch().count());
                        v->set_nano_adjustment(p.subsecond().count());
                    }
                    break;
                }
                default: break;
            }
            break;
        }
        case yugawara::storage::column_value_kind::sequence: {
            auto& value = c.default_value().element<yugawara::storage::column_value_kind::sequence>();
            auto seq = col->mutable_identity_next();
            seq->mutable_name()->mutable_element_name()->assign(value->simple_name());
            if(value->definition_id().has_value()) {
                seq->set_definition_id(*value->definition_id());
            }
            seq->set_increment_value(value->increment_value());
            seq->set_initial_value(value->initial_value());
            seq->set_max_value(value->max_value());
            seq->set_min_value(value->min_value());
            seq->set_cycle(value->cycle());
            break;
        }
    }
}
bool serialize_table(yugawara::storage::table const& t, proto::metadata::storage::TableDefinition& tbl) {
    if(t.definition_id().has_value()) {
        tbl.set_definition_id(*t.definition_id());
    }
    tbl.mutable_name()->mutable_element_name()->assign(t.simple_name());
    auto* cols = tbl.mutable_columns();
    for(auto&& c : t.columns()) {
        auto* col = cols->Add();
        col->mutable_name()->assign(c.simple_name());
        col->set_nullable(c.criteria().nullity().nullable());
        set_type(col, c);
        set_default(col, c);
    }
    return true;
}

::jogasaki::proto::metadata::storage::Direction from(yugawara::storage::sort_direction direction) {
    switch(direction) {
        case takatori::relation::sort_direction::ascendant: return ::jogasaki::proto::metadata::storage::Direction::ASCEND;
        case takatori::relation::sort_direction::descendant: return ::jogasaki::proto::metadata::storage::Direction::DESCEND;
    }
    fail();
}

::jogasaki::proto::metadata::storage::IndexFeature from(yugawara::storage::index_feature f) {
    switch(f) {
        case yugawara::storage::index_feature::primary: return ::jogasaki::proto::metadata::storage::IndexFeature::PRIMARY;
        case yugawara::storage::index_feature::find: return ::jogasaki::proto::metadata::storage::IndexFeature::FIND;
        case yugawara::storage::index_feature::scan: return ::jogasaki::proto::metadata::storage::IndexFeature::SCAN;
        case yugawara::storage::index_feature::unique: return ::jogasaki::proto::metadata::storage::IndexFeature::UNIQUE;
        case yugawara::storage::index_feature::unique_constraint: return ::jogasaki::proto::metadata::storage::IndexFeature::UNIQUE_CONSTRAINT;
    }
    fail();
}

bool serialize_index(yugawara::storage::index const& idx, proto::metadata::storage::IndexDefinition& idef) {
    if(idx.definition_id().has_value()) {
        idef.set_definition_id(*idx.definition_id());
    }
    auto* name = idef.mutable_name();
    name->mutable_element_name()->assign(idx.simple_name());

    auto* keys = idef.mutable_keys();
    for(auto&& k : idx.keys()) {
        auto* ic = keys->Add();
        ic->mutable_name()->assign(k.column().simple_name());
        ic->set_direction(from(k.direction()));
    }
    auto* values = idef.mutable_values();
    for(auto&& v : idx.values()) {
        values->Add()->assign(static_cast<yugawara::storage::column const&>(v).simple_name());
    }

    auto* features = idef.mutable_index_features();
    for(auto&& f : idx.features()) {
        features->Add(from(f));
    }
    return true;
}

} // namespace details

bool storage_metadata_serializer::serialize(yugawara::storage::index const& idx, proto::metadata::storage::IndexDefinition& idef) {
    bool is_primary = idx.table().simple_name() == idx.simple_name();
    if(is_primary) {
        auto* tdef = idef.mutable_table_definition();
        auto& t = idx.table();
        if(! details::serialize_table(t, *tdef)) {
            return false;
        }
        if(! details::serialize_index(idx, idef)) {
            return false;
        }
    } else {
        if(! details::serialize_index(idx, idef)) {
            return false;
        }
        idef.mutable_table_reference()->mutable_name()->mutable_element_name()->assign(idx.table().simple_name());
    }
    return true;
}

bool storage_metadata_serializer::serialize(yugawara::storage::index const& idx, std::string& out) {
    proto::metadata::storage::IndexDefinition idef{};
    if(! serialize(idx, idef)) {
        VLOG(log_error) << "serialization failed";
        return false;
    }

    std::stringstream ss{};
    if (! idef.SerializeToOstream(&ss)) {
        VLOG(log_error) << "serialization failed";
        return false;
    }
    out = ss.str();
    return true;
}

std::shared_ptr<takatori::type::data const> type(::jogasaki::proto::metadata::storage::TableColumn const& column) {
    std::shared_ptr<takatori::type::data const> type{};
    switch(column.type().atom_type()) {
        case proto::metadata::common::BOOLEAN: type = std::make_shared<takatori::type::boolean>(); break;
        case proto::metadata::common::INT1: type = std::make_shared<takatori::type::int1>(); break;
        case proto::metadata::common::INT2: type = std::make_shared<takatori::type::int2>(); break;
        case proto::metadata::common::INT4: type = std::make_shared<takatori::type::int4>(); break;
        case proto::metadata::common::INT8: type = std::make_shared<takatori::type::int8>(); break;
        case proto::metadata::common::FLOAT4: type = std::make_shared<takatori::type::float4>(); break;
        case proto::metadata::common::FLOAT8: type = std::make_shared<takatori::type::float8>(); break;
        case proto::metadata::common::DECIMAL: {
            std::optional<std::size_t> precision{};
            std::optional<std::size_t> scale{};
            if(column.type().has_decimal_option()) {
                auto& opt = column.type().decimal_option();
                if(opt.precision_optional_case() != proto::metadata::common::DecimalTypeOption::PRECISION_OPTIONAL_NOT_SET) {
                    precision = opt.precision();
                }
                if(opt.scale_optional_case() != proto::metadata::common::DecimalTypeOption::SCALE_OPTIONAL_NOT_SET) {
                    scale = opt.scale();
                }
            }
            type = std::make_shared<takatori::type::decimal>(precision, scale);
            break;
        }
        case proto::metadata::common::CHARACTER: {
            bool varying = false;
            std::optional<std::size_t> length{};
            if(column.type().has_character_option()) {
                auto& opt = column.type().character_option();
                varying = opt.varying();
                if(opt.length_optional_case() != proto::metadata::common::CharacterTypeOption::LENGTH_OPTIONAL_NOT_SET) {
                    length = opt.length();
                }
            }
            type = std::make_shared<takatori::type::character>(takatori::type::varying_t{varying}, length);
            break;
        }
        case proto::metadata::common::OCTET: {
            bool varying = false;
            std::optional<std::size_t> length{};
            if(column.type().has_octet_option()) {
                auto& opt = column.type().octet_option();
                varying = opt.varying();
                if(opt.length_optional_case() != proto::metadata::common::OctetTypeOption::LENGTH_OPTIONAL_NOT_SET) {
                    length = opt.length();
                }
            }
            type = std::make_shared<takatori::type::octet>(takatori::type::varying_t{varying}, length);
            break;
        }
        case proto::metadata::common::DATE: type = std::make_shared<takatori::type::date>(); break;
        case proto::metadata::common::TIME_OF_DAY: {
            type = std::make_shared<takatori::type::time_of_day>(~takatori::type::with_time_zone);
            break;
        }
        case proto::metadata::common::TIME_POINT: {
            type = std::make_shared<takatori::type::time_point>(~takatori::type::with_time_zone);
            break;
        }
        case proto::metadata::common::TIME_OF_DAY_WITH_TIME_ZONE: {
            type = std::make_shared<takatori::type::time_of_day>(takatori::type::with_time_zone);
            break;
        }
        case proto::metadata::common::TIME_POINT_WITH_TIME_ZONE: {
            type = std::make_shared<takatori::type::time_point>(takatori::type::with_time_zone);
            break;
        }
        case proto::metadata::common::UNKNOWN: type = std::make_shared<takatori::type::unknown>(); break;
        default: break;
    }
    return type;
}

takatori::decimal::triple to_triple(::jogasaki::proto::metadata::common::Decimal const& arg) {
    std::string_view buf{arg.unscaled_value()};
    auto exp = arg.exponent();
    return utils::read_decimal(buf, -exp);
}

yugawara::storage::column_value default_value(
    ::jogasaki::proto::metadata::storage::TableColumn const& column,
    yugawara::storage::configurable_provider& provider
) {
    using yugawara::storage::column_value;
    switch(column.default_value_case()) {
        case proto::metadata::storage::TableColumn::kBooleanValue: return column_value{std::make_shared<takatori::value::boolean const>(column.boolean_value())};
        case proto::metadata::storage::TableColumn::kInt4Value: return column_value{std::make_shared<takatori::value::int4 const>(column.int4_value())};
        case proto::metadata::storage::TableColumn::kInt8Value: return column_value{std::make_shared<takatori::value::int8 const>(column.int8_value())};
        case proto::metadata::storage::TableColumn::kFloat4Value: return column_value{std::make_shared<takatori::value::float4 const>(column.float4_value())};
        case proto::metadata::storage::TableColumn::kFloat8Value: return column_value{std::make_shared<takatori::value::float8 const>(column.float8_value())};
        case proto::metadata::storage::TableColumn::kDecimalValue: {
            auto& v = column.decimal_value();
            return column_value{std::make_shared<takatori::value::decimal const>(to_triple(v))};
        }
        case proto::metadata::storage::TableColumn::kCharacterValue: return column_value{std::make_shared<takatori::value::character const>(column.character_value())};
        case proto::metadata::storage::TableColumn::kOctetValue: return column_value{std::make_shared<takatori::value::octet const>(column.octet_value())};
        case proto::metadata::storage::TableColumn::kDateValue: {
            return column_value{std::make_shared<takatori::value::date const>(takatori::datetime::date{column.date_value()})};
        }
        case proto::metadata::storage::TableColumn::kTimeOfDayValue: {
            return column_value{std::make_shared<takatori::value::time_of_day const>(takatori::datetime::time_of_day{std::chrono::duration<std::uint64_t, std::nano>(column.time_of_day_value())})};
        }
        case proto::metadata::storage::TableColumn::kTimePointValue: {
            auto& v = column.time_point_value();
            return column_value{std::make_shared<takatori::value::time_point const>(takatori::datetime::time_point{std::chrono::duration<std::int64_t>{v.offset_seconds()}, std::chrono::nanoseconds{v.nano_adjustment()}})};
        }
        case proto::metadata::storage::TableColumn::kTimeOfDayWithTimeZoneValue: {
            auto& v = column.time_of_day_with_time_zone_value();
            return column_value{std::make_shared<takatori::value::time_of_day const>(takatori::datetime::time_of_day{std::chrono::duration<std::uint64_t, std::nano>(v.offset_nanoseconds())})};
        }
        case proto::metadata::storage::TableColumn::kTimePointWithTimeZoneValue: {
            auto& v = column.time_point_with_time_zone_value();
            return column_value{std::make_shared<takatori::value::time_point const>(takatori::datetime::time_point{std::chrono::duration<std::int64_t>{v.offset_seconds()}, std::chrono::nanoseconds{v.nano_adjustment()}})};
        }
        case proto::metadata::storage::TableColumn::kSequenceNext: return {}; //TODO
        case proto::metadata::storage::TableColumn::kIdentityNext: {
            auto& v = column.identity_next();
            BOOST_ASSERT(v.has_name());  //NOLINT
            auto seq = std::make_shared<yugawara::storage::sequence>(
                v.name().element_name(),
                v.initial_value(),
                v.increment_value(),
                v.min_value(),
                v.max_value(),
                v.cycle()
            );
            if(v.definition_id_optional_case() != proto::metadata::storage::SequenceDefinition::DEFINITION_ID_OPTIONAL_NOT_SET) {
                seq->definition_id(v.definition_id());
            }
            provider.add_sequence(seq);
            return column_value{std::move(seq)};
        }
        case proto::metadata::storage::TableColumn::DEFAULT_VALUE_NOT_SET: break;
        default: break;
    }
    return {};
}

yugawara::storage::column from(::jogasaki::proto::metadata::storage::TableColumn const& column, yugawara::storage::configurable_provider& provider) {
    yugawara::variable::criteria criteria{yugawara::variable::nullity{column.nullable()}};
    return yugawara::storage::column{
        column.name(),
        type(column),
        std::move(criteria),
        default_value(column, provider)
    };
}

// deserialize table and add its depending definitions (base table, and sequence) to the provider
bool deserialize_table(
    ::jogasaki::proto::metadata::storage::TableDefinition const& tdef,
    std::shared_ptr<yugawara::storage::table>& out,
    yugawara::storage::configurable_provider& provider
) {
    std::optional<yugawara::storage::table::definition_id_type> definition_id{};
    if(tdef.definition_id_optional_case() != proto::metadata::storage::TableDefinition::DefinitionIdOptionalCase::DEFINITION_ID_OPTIONAL_NOT_SET) {
        definition_id = tdef.definition_id();
    }
    takatori::util::reference_vector<yugawara::storage::column> columns{};
    for(auto&& c : tdef.columns()) {
        columns.emplace_back(from(c, provider));
    }
    if(! tdef.has_name()) {
        return false;
    }
    out = provider.add_table(std::make_shared<yugawara::storage::table>(
        definition_id,
        tdef.name().element_name(),
        std::move(columns)
    ));
    return true;
}

takatori::relation::sort_direction direction(::jogasaki::proto::metadata::storage::Direction dir) {
    switch(dir) {
        case proto::metadata::storage::ASCEND: return takatori::relation::sort_direction::ascendant;
        case proto::metadata::storage::DESCEND: return takatori::relation::sort_direction::descendant;
        default: return takatori::relation::sort_direction::ascendant;
    }
    fail();
}

yugawara::storage::column const* find_column(yugawara::storage::table const& tbl, std::string_view name) {
    for(auto&& c : tbl.columns()) {
        if(c.simple_name() == name) {
            return std::addressof(c);
        }
    }
    return nullptr;
}

yugawara::storage::index_feature_set features(::jogasaki::proto::metadata::storage::IndexDefinition const& idef) {
    yugawara::storage::index_feature_set ret{};
    for(auto&& f : idef.index_features()) {
        switch(f) {
            case ::jogasaki::proto::metadata::storage::IndexFeature::PRIMARY: ret.insert(yugawara::storage::index_feature::primary); break;
            case ::jogasaki::proto::metadata::storage::IndexFeature::FIND: ret.insert(yugawara::storage::index_feature::find); break;
            case ::jogasaki::proto::metadata::storage::IndexFeature::SCAN: ret.insert(yugawara::storage::index_feature::scan); break;
            case ::jogasaki::proto::metadata::storage::IndexFeature::UNIQUE: ret.insert(yugawara::storage::index_feature::unique); break;
            case ::jogasaki::proto::metadata::storage::IndexFeature::UNIQUE_CONSTRAINT: ret.insert(yugawara::storage::index_feature::unique_constraint); break;
            default: break;
        }
    }
    return ret;
}

bool deserialize_index(
    ::jogasaki::proto::metadata::storage::IndexDefinition const& idef,
    std::shared_ptr<yugawara::storage::table const> tbl,
    std::shared_ptr<yugawara::storage::index>& out
) {
    std::optional<yugawara::storage::index::definition_id_type> definition_id{};
    if(idef.definition_id_optional_case() != proto::metadata::storage::IndexDefinition::DefinitionIdOptionalCase::DEFINITION_ID_OPTIONAL_NOT_SET) {
        definition_id = idef.definition_id();
    }

    std::vector<yugawara::storage::index::key> keys{};
    for(auto&& k : idef.keys()) {
        auto* c = find_column(*tbl, k.name());
        if(c == nullptr) {
            VLOG(log_error) << "key column '" << k.name() << "' not found in the base table";
            return false;
        }
        keys.emplace_back(*c ,direction(k.direction()));
    }

    std::vector<yugawara::storage::index::column_ref> values{};
    for(auto&& v : idef.values()) {
        auto* c = find_column(*tbl, v);
        if(c == nullptr) {
            VLOG(log_error) << "value column '" << v << "' not found in the base table";
            return false;
        }
        values.emplace_back(*c);
    }

    std::string_view simple_name = idef.has_name() ? idef.name().element_name() : std::string_view{};

    out = std::make_shared<yugawara::storage::index>(
        definition_id,
        std::move(tbl),
        std::string{simple_name},
        std::move(keys),
        std::move(values),
        features(idef)
    );
    return true;
}

bool storage_metadata_serializer::deserialize(
    std::string_view src,
    yugawara::storage::configurable_provider const& in,
    yugawara::storage::configurable_provider& out
) {
    proto::metadata::storage::IndexDefinition idef{};
    if (! idef.ParseFromArray(src.data(), src.size())) {
        VLOG(log_error) << "storage metadata deserialize: parse error";
        return false;
    }
    return deserialize(idef, in, out);
}

bool storage_metadata_serializer::deserialize(
    proto::metadata::storage::IndexDefinition const& idef,
    yugawara::storage::configurable_provider const& in,
    yugawara::storage::configurable_provider& out
) {
    if(idef.has_table_definition()) {
        // primary index
        auto& tdef = idef.table_definition();
        std::shared_ptr<yugawara::storage::table> tbl{};
        if(! deserialize_table(tdef, tbl, out)) {
            return false;
        }
        std::shared_ptr<yugawara::storage::index> idx{};
        if(! deserialize_index(idef, tbl, idx)) {
            return false;
        }
        out.add_index(idx);
        return true;
    }
    // secondary index
    if(! idef.has_table_reference()) {
        return false;
    }
    auto& tabname = idef.table_reference().name().element_name();
    std::shared_ptr<yugawara::storage::table const> t{};
    if(t = out.find_table(tabname); t == nullptr) {
        if(t = in.find_table(idef.table_reference().name().element_name()); t == nullptr) {
            return false;
        }
    }
    std::shared_ptr<yugawara::storage::index> idx{};
    if(! deserialize_index(idef, t, idx)) {
        return false;
    }
    out.add_index(idx);
    return true;
}
}

