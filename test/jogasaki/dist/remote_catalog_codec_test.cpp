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
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <gtest/gtest.h>

#include <takatori/relation/sort_direction.h>
#include <takatori/type/blob.h>
#include <takatori/type/character.h>
#include <takatori/type/clob.h>
#include <takatori/type/date.h>
#include <takatori/type/datetime_interval.h>
#include <takatori/type/decimal.h>
#include <takatori/type/octet.h>
#include <takatori/type/primitive.h>
#include <takatori/type/row_id.h>
#include <takatori/type/table.h>
#include <takatori/type/time_of_day.h>
#include <takatori/type/time_point.h>
#include <takatori/type/varying.h>
#include <takatori/type/with_time_zone.h>
#include <takatori/util/reference_vector.h>
#include <yugawara/aggregate/configurable_provider.h>
#include <yugawara/aggregate/declaration.h>
#include <yugawara/function/configurable_provider.h>
#include <yugawara/function/declaration.h>
#include <yugawara/schema/catalog.h>
#include <yugawara/schema/configurable_provider.h>
#include <yugawara/schema/declaration.h>
#include <yugawara/schema/provider.h>
#include <yugawara/storage/column.h>
#include <yugawara/storage/configurable_provider.h>
#include <yugawara/storage/index.h>
#include <yugawara/storage/provider.h>
#include <yugawara/storage/table.h>
#include <yugawara/variable/criteria.h>
#include <yugawara/variable/nullity.h>
#include <tateyama/proto/distributed_query/remote_catalog.pb.h>

#include <jogasaki/dist/remote_catalog_decoder.h>
#include <jogasaki/dist/remote_catalog_encoder.h>
#include <jogasaki/dist/remote_catalog_information.h>
#include <jogasaki/dist/remote_catalog_manager.h>
#include <jogasaki/dist/remote_function_id_registry.h>
#include <jogasaki/dist/remote_server_channel_manager.h>

namespace jogasaki::testing {
namespace {

using namespace dist;
namespace t = takatori::type;
namespace wire = ::tateyama::grpc::proto;
using type_ptr = remote_catalog_information::type_ptr;
using yugawara::function::function_feature;
using yugawara::storage::index_feature;

type_ptr int8_type() { return std::make_shared<t::int8>(); }

std::optional<std::size_t> size(std::size_t value) { return value; }

template <class Set>
std::vector<int> as_vector(Set const& features) {
    std::vector<int> result{};
    for (auto feature : features) result.push_back(static_cast<int>(feature));
    return result;
}

template <class Element>
Element const& named(std::vector<Element> const& elements, std::string_view name) {
    for (auto const& e : elements) {
        if (e.name == name) return e;
    }
    throw std::out_of_range{std::string{name}};
}

std::shared_ptr<yugawara::schema::catalog> catalog_of(
    std::shared_ptr<yugawara::storage::configurable_provider> storage,
    std::shared_ptr<yugawara::function::configurable_provider> functions = {},
    std::shared_ptr<yugawara::aggregate::configurable_provider> aggregates = {}
) {
    if (!functions) functions = std::make_shared<yugawara::function::configurable_provider>();
    if (!aggregates) aggregates = std::make_shared<yugawara::aggregate::configurable_provider>();
    auto schemas = std::make_shared<yugawara::schema::configurable_provider>();
    schemas->add(yugawara::schema::declaration{std::nullopt, "public", std::move(storage), {},
        std::move(functions), std::move(aggregates)});
    return std::make_shared<yugawara::schema::catalog>("source", std::nullopt, std::move(schemas));
}

class fixed_fetcher final : public remote_catalog_fetcher {
public:
    explicit fixed_fetcher(remote_catalog_information info) : info_(std::move(info)) {}
    status fetch(remote_catalog_reference const&, std::shared_ptr<grpc::Channel> const&,
        std::chrono::system_clock::time_point, remote_catalog_information& out, std::string&) override {
        out = info_;
        return status::ok;
    }
private:
    remote_catalog_information info_;
};

// a table-valued return type nesting every column attribute the RPC must keep
type_ptr tvf_return_type() {
    std::vector<t::table::column_type> columns{};
    columns.emplace_back("a", int8_type());
    columns.emplace_back("b", std::make_shared<t::decimal>(size(10), size(2)));
    columns.emplace_back("c", std::make_shared<t::character>(t::varying_t{true}, size(20)));
    columns.emplace_back("d", std::make_shared<t::time_point>(t::with_time_zone_t{true}));
    return std::make_shared<t::table>(std::move(columns));
}

/**
 * @brief a source catalog with every remote execution declaration kind.
 * @details Column types cover the attributes of RemoteDataType; function signatures cover the types that only
 * appear in declarations (int1, int2, boolean, unknown, row_id, nested table).
 */
std::shared_ptr<yugawara::schema::catalog> source_catalog() {
    auto storage = std::make_shared<yugawara::storage::configurable_provider>();
    auto table = std::make_shared<yugawara::storage::table>(
        std::optional<yugawara::storage::table::definition_id_type>{41}, "t",
        takatori::util::reference_vector<yugawara::storage::column>{}, std::string{});
    auto add_column = [&](std::string name, type_ptr type, bool nullable) {
        table->columns().emplace_back(std::move(name), std::move(type),
            yugawara::variable::criteria{yugawara::variable::nullity{nullable}},
            yugawara::storage::column_value{}, yugawara::storage::column_feature_set{});
    };
    add_column("k", int8_type(), false);
    add_column("dec", std::make_shared<t::decimal>(size(10), size(2)), true);
    add_column("flexible", std::make_shared<t::decimal>(std::nullopt, std::nullopt), true);
    add_column("vc", std::make_shared<t::character>(t::varying_t{true}, size(20)), true);
    add_column("vc_any", std::make_shared<t::character>(t::varying_t{true}, std::nullopt), true);
    add_column("ch", std::make_shared<t::character>(t::varying_t{false}, size(5)), true);
    add_column("bin", std::make_shared<t::octet>(t::varying_t{true}, std::nullopt), true);
    add_column("tod", std::make_shared<t::time_of_day>(t::with_time_zone_t{true}), true);
    add_column("tp", std::make_shared<t::time_point>(t::with_time_zone_t{false}), true);
    add_column("dt", std::make_shared<t::date>(), true);
    add_column("bl", std::make_shared<t::blob>(), true);
    add_column("cl", std::make_shared<t::clob>(), true);
    add_column("f4", std::make_shared<t::float4>(), true);
    storage->add_table(table);
    std::vector<yugawara::storage::index::key> primary_keys{};
    primary_keys.emplace_back(table->columns()[0], takatori::relation::sort_direction::ascendant);
    std::vector<yugawara::storage::index::column_ref> primary_values{};
    primary_values.emplace_back(table->columns()[1]);
    storage->add_index(yugawara::storage::index{
        std::optional<yugawara::storage::index::definition_id_type>{42}, table, "t", std::move(primary_keys),
        std::move(primary_values),
        {index_feature::primary, index_feature::find, index_feature::scan, index_feature::unique}, std::string{}});
    std::vector<yugawara::storage::index::key> secondary_keys{};
    secondary_keys.emplace_back(table->columns()[3], takatori::relation::sort_direction::descendant);
    secondary_keys.emplace_back(table->columns()[2], takatori::relation::sort_direction::ascendant);
    storage->add_index(yugawara::storage::index{
        std::optional<yugawara::storage::index::definition_id_type>{43}, table, "t_vc", std::move(secondary_keys),
        {}, {index_feature::find, index_feature::scan, index_feature::unique_constraint}, std::string{}});

    auto functions = std::make_shared<yugawara::function::configurable_provider>();
    std::vector<type_ptr> scalar_parameters{std::make_shared<t::int1>(), std::make_shared<t::int2>(),
        std::make_shared<t::boolean>(), std::make_shared<t::unknown>(), std::make_shared<t::row_id>(3)};
    functions->add(yugawara::function::declaration{77, "f", std::make_shared<t::character>(t::varying_t{true}, std::nullopt),
        scalar_parameters, {function_feature::scalar_function}});
    functions->add(yugawara::function::declaration{79, "tvf", tvf_return_type(), std::vector<type_ptr>{int8_type()},
        {function_feature::table_valued_function}});
    auto aggregates = std::make_shared<yugawara::aggregate::configurable_provider>();
    aggregates->add(yugawara::aggregate::declaration{78, "agg_incremental", int8_type(),
        std::vector<type_ptr>{int8_type()}, true});
    aggregates->add(yugawara::aggregate::declaration{80, "agg_plain", int8_type(),
        std::vector<type_ptr>{int8_type()}, false});
    return catalog_of(storage, functions, aggregates);
}

// Compare transferred table columns explicitly, including every nested type attribute.
void expect_same_type(t::data const& expected, t::data const& actual) {
    ASSERT_EQ(expected.kind(), actual.kind());
    if (expected.kind() == t::type_kind::table) {
        auto const& a = static_cast<t::table const&>(expected).columns();
        auto const& b = static_cast<t::table const&>(actual).columns();
        ASSERT_EQ(a.size(), b.size());
        for (std::size_t i = 0; i < a.size(); ++i) {
            EXPECT_EQ(a[i].name(), b[i].name());
            expect_same_type(a[i].type(), b[i].type());
        }
    } else {
        EXPECT_TRUE(expected == actual) << expected << " != " << actual;
    }
}

void expect_same_types(std::vector<type_ptr> const& a, std::vector<type_ptr> const& b) {
    ASSERT_EQ(a.size(), b.size());
    for (std::size_t i = 0; i < a.size(); ++i) {
        ASSERT_TRUE(a[i]);
        ASSERT_TRUE(b[i]);
        expect_same_type(*a[i], *b[i]);
    }
}

wire::RemoteCatalog encoded_source() {
    wire::RemoteCatalog message{};
    std::string diagnostic{};
    EXPECT_EQ(status::ok, remote_catalog_encoder{}.encode(*source_catalog(), message, diagnostic)) << diagnostic;
    return message;
}

TEST(remote_catalog_codec_test, round_trip_preserves_types_features_and_remote_ids) {
    auto source = source_catalog();
    remote_catalog_information expected{};
    std::string diagnostic{};
    ASSERT_EQ(status::ok, describe_remote_catalog(*source, expected, diagnostic)) << diagnostic;
    auto message = encoded_source();

    // wire form: remote IDs as given, indices 0-origin, optional indices present
    ASSERT_EQ(1, message.table_list_size());
    EXPECT_EQ(41U, message.table_list(0).remote_definition_id());
    ASSERT_EQ(2, message.index_list_size());
    for (auto const& index : message.index_list()) {
        EXPECT_EQ(wire::RemoteIndex::kTableIndex, index.table_index_presence_case());
        EXPECT_EQ(0U, index.table_index());
        for (auto const& key : index.keys()) EXPECT_EQ(wire::RemoteIndexKey::kColumnIndex, key.column_index_presence_case());
    }
    EXPECT_TRUE(message.column_list(2).data_type().has_decimal_type());
    EXPECT_EQ(wire::RemoteDecimalType::PRECISION_PRESENCE_NOT_SET,
        message.column_list(2).data_type().decimal_type().precision_presence_case());
    EXPECT_EQ(wire::RemoteDecimalType::SCALE_PRESENCE_NOT_SET,
        message.column_list(2).data_type().decimal_type().scale_presence_case());
    EXPECT_EQ(20U, message.column_list(3).data_type().character_type().length());
    EXPECT_EQ(wire::RemoteVaryingLengthType::LENGTH_PRESENCE_NOT_SET,
        message.column_list(4).data_type().character_type().length_presence_case());
    EXPECT_FALSE(message.column_list(5).data_type().character_type().varying());

    remote_catalog_information decoded{};
    ASSERT_EQ(status::ok, remote_catalog_decoder{}.decode(message, decoded, diagnostic)) << diagnostic;

    ASSERT_EQ(expected.schemas.size(), decoded.schemas.size());
    EXPECT_EQ("public", decoded.schemas[0].name);
    EXPECT_EQ(expected.schemas[0].tables, decoded.schemas[0].tables);
    EXPECT_EQ(expected.schemas[0].indices, decoded.schemas[0].indices);
    EXPECT_EQ(expected.schemas[0].functions, decoded.schemas[0].functions);
    EXPECT_EQ(expected.schemas[0].aggregate_functions, decoded.schemas[0].aggregate_functions);
    ASSERT_EQ(expected.columns.size(), decoded.columns.size());
    for (std::size_t i = 0; i < expected.columns.size(); ++i) {
        EXPECT_EQ(expected.columns[i].name, decoded.columns[i].name);
        EXPECT_EQ(expected.columns[i].nullable, decoded.columns[i].nullable);
        EXPECT_TRUE(*expected.columns[i].type == *decoded.columns[i].type) << expected.columns[i].name;
    }
    EXPECT_EQ(expected.tables[0].columns, decoded.tables[0].columns);
    ASSERT_EQ(expected.indices.size(), decoded.indices.size());
    for (std::size_t i = 0; i < expected.indices.size(); ++i) {
        auto const& e = expected.indices[i];
        auto const& d = decoded.indices[i];
        EXPECT_EQ(e.remote_definition_id, d.remote_definition_id);
        EXPECT_EQ(e.name, d.name);
        EXPECT_EQ(e.table, d.table);
        EXPECT_EQ(as_vector(e.features), as_vector(d.features));
        ASSERT_EQ(e.keys.size(), d.keys.size());
        for (std::size_t k = 0; k < e.keys.size(); ++k) {
            EXPECT_EQ(e.keys[k].column, d.keys[k].column);
            EXPECT_TRUE(e.keys[k].direction == d.keys[k].direction);
        }
        EXPECT_EQ(e.values, d.values);
    }
    // unique and unique_constraint are kept apart and nothing is completed
    auto const& secondary = named(decoded.indices, "t_vc");
    EXPECT_EQ(43U, secondary.remote_definition_id);
    EXPECT_TRUE(secondary.features.contains(index_feature::unique_constraint));
    EXPECT_FALSE(secondary.features.contains(index_feature::unique));
    EXPECT_FALSE(secondary.features.contains(index_feature::primary));
    ASSERT_EQ(2U, secondary.keys.size());
    EXPECT_TRUE(secondary.keys[0].direction == takatori::relation::sort_direction::descendant);
    EXPECT_EQ(42U, named(decoded.indices, "t").remote_definition_id);
    EXPECT_EQ(77U, named(decoded.functions, "f").remote_definition_id);
    EXPECT_TRUE(named(decoded.functions, "tvf").features.contains(function_feature::table_valued_function));
    expect_same_type(*tvf_return_type(), *named(decoded.functions, "tvf").return_type);
    EXPECT_TRUE(named(decoded.aggregate_functions, "agg_incremental").incremental);
    EXPECT_FALSE(named(decoded.aggregate_functions, "agg_plain").incremental);
    ASSERT_EQ(expected.functions.size(), decoded.functions.size());
    for (std::size_t i = 0; i < expected.functions.size(); ++i) {
        EXPECT_EQ(expected.functions[i].remote_definition_id, decoded.functions[i].remote_definition_id);
        EXPECT_EQ(as_vector(expected.functions[i].features), as_vector(decoded.functions[i].features));
        expect_same_types(expected.functions[i].parameter_types, decoded.functions[i].parameter_types);
        expect_same_type(*expected.functions[i].return_type, *decoded.functions[i].return_type);
    }
    ASSERT_EQ(expected.aggregate_functions.size(), decoded.aggregate_functions.size());
    for (std::size_t i = 0; i < expected.aggregate_functions.size(); ++i) {
        EXPECT_EQ(expected.aggregate_functions[i].remote_definition_id, decoded.aggregate_functions[i].remote_definition_id);
        EXPECT_EQ(expected.aggregate_functions[i].incremental, decoded.aggregate_functions[i].incremental);
        expect_same_types(expected.aggregate_functions[i].parameter_types, decoded.aggregate_functions[i].parameter_types);
    }
}

TEST(remote_catalog_codec_test, decoded_catalog_is_restored_by_manager_with_new_function_ids) {
    auto message = encoded_source();
    remote_catalog_information decoded{};
    std::string diagnostic{};
    ASSERT_EQ(status::ok, remote_catalog_decoder{}.decode(message, decoded, diagnostic)) << diagnostic;
    auto ids = std::make_shared<remote_function_id_registry>();
    remote_catalog_manager manager{std::make_shared<fixed_fetcher>(decoded),
        remote_catalog_manager::default_fetch_timeout, ids};
    remote_server_channel_manager channels{};
    ASSERT_EQ(status::ok, channels.start({{"worker", "dns:///localhost:12345", false, root_certificate_source::none, {}}}));
    ASSERT_EQ(status::ok, manager.start());
    auto rc = manager.create({"remote", "worker"}, channels, diagnostic);
    // row_id is outside the remote execution signature types; the coordinator rejects the whole catalog
    EXPECT_NE(status::ok, rc);
    EXPECT_FALSE(manager.find_catalog("remote"));
    EXPECT_EQ(0U, ids->reserved_count());

    for (auto& function : decoded.functions) {
        if (function.name == "f") function.parameter_types.pop_back();
    }
    remote_catalog_manager accepted{std::make_shared<fixed_fetcher>(decoded),
        remote_catalog_manager::default_fetch_timeout, ids};
    ASSERT_EQ(status::ok, accepted.start());
    ASSERT_EQ(status::ok, accepted.create({"remote", "worker"}, channels, diagnostic)) << diagnostic;
    auto schema = accepted.find_catalog("remote")->schema_provider().find("public");
    ASSERT_TRUE(schema);
    auto table = schema->storage_provider().find_table("t");
    ASSERT_TRUE(table);
    EXPECT_EQ(41U, accepted.snapshot()->remote_definition(*table)->remote_definition_id);
    auto index = schema->storage_provider().find_index("t_vc");
    ASSERT_TRUE(index);
    EXPECT_EQ(43U, accepted.snapshot()->remote_definition(*index)->remote_definition_id);
    EXPECT_TRUE(index->features().contains(index_feature::unique_constraint));
    ASSERT_EQ(2U, index->keys().size());
    EXPECT_EQ("vc", index->keys()[0].column().simple_name());
    EXPECT_EQ(takatori::relation::sort_direction::descendant, index->keys()[0].direction());
    EXPECT_EQ(4U, ids->reserved_count());
}

TEST(remote_catalog_codec_test, optional_indices_distinguish_absent_from_zero) {
    auto message = encoded_source();
    message.mutable_index_list(0)->clear_table_index();
    message.mutable_index_list(0)->mutable_keys(0)->clear_column_index();
    remote_catalog_information decoded{};
    std::string diagnostic{};
    ASSERT_EQ(status::ok, remote_catalog_decoder{}.decode(message, decoded, diagnostic)) << diagnostic;
    EXPECT_FALSE(decoded.indices[0].table);
    EXPECT_FALSE(decoded.indices[0].keys[0].column);
    ASSERT_TRUE(decoded.indices[1].table);
    EXPECT_EQ(0U, *decoded.indices[1].table);
}

TEST(remote_catalog_codec_test, missing_definition_id_is_transferred_as_zero) {
    auto storage = std::make_shared<yugawara::storage::configurable_provider>();
    auto table = std::make_shared<yugawara::storage::table>("no_id",
        takatori::util::reference_vector<yugawara::storage::column>{});
    table->columns().emplace_back("c", int8_type(), yugawara::variable::criteria{yugawara::variable::nullity{false}},
        yugawara::storage::column_value{}, yugawara::storage::column_feature_set{});
    storage->add_table(table);
    wire::RemoteCatalog message{};
    ASSERT_EQ(status::ok, remote_catalog_encoder{}.encode(*catalog_of(storage), message));
    ASSERT_EQ(1, message.table_list_size());
    EXPECT_EQ(0U, message.table_list(0).remote_definition_id());
    EXPECT_EQ(0, message.index_list_size());
}

TEST(remote_catalog_codec_test, encoder_rejects_interval_and_leaves_destination_unchanged) {
    auto functions = std::make_shared<yugawara::function::configurable_provider>();
    functions->add(yugawara::function::declaration{90, "interval_function", std::make_shared<t::datetime_interval>(),
        std::vector<type_ptr>{}, {function_feature::scalar_function}});
    auto catalog = catalog_of(std::make_shared<yugawara::storage::configurable_provider>(), functions);
    wire::RemoteCatalog destination{};
    destination.add_schema_list()->set_schema_name("previous");
    std::string diagnostic{};
    EXPECT_EQ(status::err_unsupported, remote_catalog_encoder{}.encode(*catalog, destination, diagnostic));
    EXPECT_FALSE(diagnostic.empty());
    ASSERT_EQ(1, destination.schema_list_size());
    EXPECT_EQ("previous", destination.schema_list(0).schema_name());
}

TEST(remote_catalog_codec_test, decoder_rejects_malformed_and_unknown_wire_values) {
    auto const base = encoded_source();
    struct invalid_case {
        char const* name;
        std::function<void(wire::RemoteCatalog&)> edit;
        status expected;
    };
    std::vector<invalid_case> cases{
        {"missing oneof", [](auto& c) { c.mutable_column_list(0)->mutable_data_type()->Clear(); },
            status::err_data_corruption},
        {"unknown oneof case", [](auto& c) {
            // field 21 of RemoteDataType: a case added by a later minor version
            ASSERT_TRUE(c.mutable_column_list(0)->mutable_data_type()->ParseFromString(std::string{"\xaa\x01\x00", 3}));
        }, status::err_unsupported},
        {"bit", [](auto& c) { c.mutable_column_list(0)->mutable_data_type()->mutable_bit_type(); },
            status::err_unsupported},
        {"interval", [](auto& c) { c.mutable_column_list(0)->mutable_data_type()->mutable_datetime_interval_type(); },
            status::err_unsupported},
        {"missing column type", [](auto& c) { c.mutable_column_list(0)->clear_data_type(); },
            status::err_data_corruption},
        {"missing return type", [](auto& c) { c.mutable_function_list(0)->clear_return_type(); },
            status::err_data_corruption},
        {"nested table column without type", [](auto& c) {
            for (auto& f : *c.mutable_function_list()) {
                if (f.return_type().has_table_type()) f.mutable_return_type()->mutable_table_type()->mutable_columns(0)->clear_data_type();
            }
        }, status::err_data_corruption},
        {"zero precision", [](auto& c) { c.mutable_column_list(1)->mutable_data_type()->mutable_decimal_type()->set_precision(0); },
            status::err_data_corruption},
        {"unspecified index feature", [](auto& c) { c.mutable_index_list(0)->add_features(wire::REMOTE_INDEX_FEATURE_UNSPECIFIED); },
            status::err_data_corruption},
        {"duplicate index feature", [](auto& c) {
            auto* i = c.mutable_index_list(0);
            i->add_features(i->features(0)); },
            status::err_data_corruption},
        {"unknown index feature", [](auto& c) {
            c.mutable_index_list(0)->add_features(static_cast<wire::RemoteIndexFeature>(99)); },
            status::err_unsupported},
        {"unspecified function feature", [](auto& c) {
            c.mutable_function_list(0)->add_features(wire::REMOTE_FUNCTION_FEATURE_UNSPECIFIED); },
            status::err_data_corruption},
        {"duplicate function feature", [](auto& c) {
            auto* f = c.mutable_function_list(0);
            f->add_features(f->features(0)); },
            status::err_data_corruption},
        {"unknown function feature", [](auto& c) {
            c.mutable_function_list(0)->add_features(static_cast<wire::RemoteFunctionFeature>(99)); },
            status::err_unsupported},
        {"unspecified sort direction", [](auto& c) {
            c.mutable_index_list(0)->mutable_keys(0)->set_direction(wire::REMOTE_SORT_DIRECTION_UNSPECIFIED); },
            status::err_data_corruption},
        {"unknown sort direction", [](auto& c) {
            c.mutable_index_list(0)->mutable_keys(0)->set_direction(static_cast<wire::RemoteSortDirection>(99)); },
            status::err_unsupported},
        {"schema table out of range", [](auto& c) { c.mutable_schema_list(0)->add_table_index_list(1); },
            status::err_data_corruption},
        {"table column out of range", [](auto& c) {
            c.mutable_table_list(0)->add_column_index_list(static_cast<std::uint64_t>(c.column_list_size())); },
            status::err_data_corruption},
        {"index table out of range", [](auto& c) { c.mutable_index_list(0)->set_table_index(1); },
            status::err_data_corruption},
        {"key column out of range", [](auto& c) { c.mutable_index_list(0)->mutable_keys(0)->set_column_index(1000); },
            status::err_data_corruption},
    };
    for (auto const& entry : cases) {
        auto message = base;
        entry.edit(message);
        remote_catalog_information destination{};
        destination.schemas.push_back({"previous", {}, {}, {}, {}});
        std::string diagnostic{};
        EXPECT_EQ(entry.expected, remote_catalog_decoder{}.decode(message, destination, diagnostic)) << entry.name;
        EXPECT_FALSE(diagnostic.empty()) << entry.name;
        ASSERT_EQ(1U, destination.schemas.size()) << entry.name;
        EXPECT_EQ("previous", destination.schemas[0].name) << entry.name;
    }
}

TEST(remote_catalog_codec_test, schema_absence_is_rejected_by_catalog_validation) {
    // wire-valid, but the tables have no owning schema (L3-02 ownership rule)
    auto message = encoded_source();
    message.clear_schema_list();
    remote_catalog_information decoded{};
    std::string diagnostic{};
    ASSERT_EQ(status::ok, remote_catalog_decoder{}.decode(message, decoded, diagnostic)) << diagnostic;
    auto ids = std::make_shared<remote_function_id_registry>();
    remote_catalog_manager manager{std::make_shared<fixed_fetcher>(decoded), remote_catalog_manager::default_fetch_timeout, ids};
    remote_server_channel_manager channels{};
    ASSERT_EQ(status::ok, channels.start({{"worker", "dns:///localhost:12345", false, root_certificate_source::none, {}}}));
    ASSERT_EQ(status::ok, manager.start());
    EXPECT_NE(status::ok, manager.create({"remote", "worker"}, channels, diagnostic));
    EXPECT_NE(std::string::npos, diagnostic.find("orphan")) << diagnostic;
    EXPECT_TRUE(manager.entries()->empty());
}

std::vector<wire::RemoteCatalogResponse> one_element_per_fragment(wire::RemoteCatalog const& catalog) {
    std::vector<wire::RemoteCatalogResponse> result{};
    auto next = [&]() -> wire::RemoteCatalog& {
        result.emplace_back();
        result.back().set_sequence_number(result.size() - 1);
        return *result.back().mutable_catalog_fragment();
    };
    for (auto const& e : catalog.schema_list()) *next().add_schema_list() = e;
    for (auto const& e : catalog.table_list()) *next().add_table_list() = e;
    for (auto const& e : catalog.column_list()) *next().add_column_list() = e;
    for (auto const& e : catalog.index_list()) *next().add_index_list() = e;
    for (auto const& e : catalog.function_list()) *next().add_function_list() = e;
    for (auto const& e : catalog.aggregate_function_list()) *next().add_aggregate_function_list() = e;
    return result;
}

TEST(remote_catalog_fragment_test, reassembles_fragments_in_sequence) {
    auto message = encoded_source();
    auto responses = one_element_per_fragment(message);
    // referenced elements (columns, tables) arrive after the schema that refers to them
    remote_catalog_fragment_assembler assembler{};
    std::string diagnostic{};
    for (auto& response : responses) {
        ASSERT_EQ(status::ok, assembler.accept(response, diagnostic)) << diagnostic;
    }
    EXPECT_EQ(responses.size(), assembler.fragments());
    remote_catalog_information whole{};
    remote_catalog_information assembled{};
    ASSERT_EQ(status::ok, remote_catalog_decoder{}.decode(message, whole, diagnostic));
    ASSERT_EQ(status::ok, assembler.finish(assembled, diagnostic)) << diagnostic;
    EXPECT_EQ(whole.columns.size(), assembled.columns.size());
    EXPECT_EQ(whole.schemas[0].tables, assembled.schemas[0].tables);
    EXPECT_EQ(whole.tables[0].columns, assembled.tables[0].columns);
    EXPECT_EQ(whole.indices[1].keys[0].column, assembled.indices[1].keys[0].column);
}

TEST(remote_catalog_fragment_test, stream_violations_discard_received_fragments) {
    auto message = encoded_source();
    struct violation {
        char const* name;
        std::function<void(std::vector<wire::RemoteCatalogResponse>&)> edit;
        status expected;
    };
    std::vector<violation> cases{
        {"sequence gap", [](auto& r) { r[1].set_sequence_number(2); }, status::err_data_corruption},
        {"sequence not from zero", [](auto& r) { for (auto& e : r) e.set_sequence_number(e.sequence_number() + 1); },
            status::err_data_corruption},
        {"missing fragment", [](auto& r) { r[1].clear_catalog_fragment(); }, status::err_data_corruption},
        {"empty fragment", [](auto& r) { r[1].mutable_catalog_fragment()->Clear(); }, status::err_data_corruption},
        {"oversized response", [](auto& r) {
            r[1].mutable_catalog_fragment()->mutable_column_list(0)->set_column_name(
                std::string(remote_catalog_max_message_size, 'x')); }, status::err_resource_limit_reached},
    };
    for (auto const& entry : cases) {
        // schema, column, table: the second response carries a column
        std::vector<wire::RemoteCatalogResponse> responses(3);
        *responses[0].mutable_catalog_fragment()->add_schema_list() = message.schema_list(0);
        *responses[1].mutable_catalog_fragment()->add_column_list() = message.column_list(0);
        *responses[2].mutable_catalog_fragment()->add_table_list() = message.table_list(0);
        for (std::size_t i = 0; i < responses.size(); ++i) responses[i].set_sequence_number(i);
        entry.edit(responses);
        remote_catalog_fragment_assembler assembler{};
        std::string diagnostic{};
        status rc = status::ok;
        for (auto& response : responses) {
            rc = assembler.accept(response, diagnostic);
            if (rc != status::ok) break;
        }
        EXPECT_EQ(entry.expected, rc) << entry.name;
        EXPECT_EQ(0U, assembler.fragments()) << entry.name;
        EXPECT_EQ(0U, assembler.bytes()) << entry.name;
        // nothing received before the violation can be used
        remote_catalog_information destination{};
        destination.schemas.push_back({"previous", {}, {}, {}, {}});
        EXPECT_EQ(status::err_invalid_state, assembler.finish(destination, diagnostic)) << entry.name;
        ASSERT_EQ(1U, destination.schemas.size());
        EXPECT_EQ("previous", destination.schemas[0].name);
    }
}

}  // namespace
}  // namespace jogasaki::testing
