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


#include <cstdlib>
#include <memory>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include <takatori/plan/graph.h>
#include <takatori/plan/process.h>
#include <takatori/plan/step_kind.h>
#include <takatori/relation/find.h>
#include <takatori/relation/scan.h>
#include <takatori/statement/execute.h>
#include <takatori/statement/statement_kind.h>
#include <takatori/type/primitive.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <yugawara/binding/extract.h>
#include <yugawara/compiler_result.h>
#include <yugawara/schema/configurable_provider.h>
#include <yugawara/storage/configurable_provider.h>
#include <yugawara/storage/index.h>
#include <yugawara/storage/index_feature.h>
#include <yugawara/storage/table.h>
#include <yugawara/variable/criteria.h>
#include <yugawara/variable/nullity.h>

#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/impl/executable_statement.h>
#include <jogasaki/configuration.h>
#include <jogasaki/dist/operation_location.h>
#include <jogasaki/dist/remote_catalog_provider.h>
#include <jogasaki/dist/symbol_locations.h>
#include <jogasaki/error/error_info.h>
#include <jogasaki/executor/common/execute.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/executor/process/step.h>
#include <jogasaki/model/step_kind.h>
#include <jogasaki/plan/compiler.h>
#include <jogasaki/plan/compiler_context.h>
#include <jogasaki/plan/prepared_statement.h>
#include <jogasaki/plan/mirror_container.h>
#include <jogasaki/storage/storage_manager.h>

namespace jogasaki::testing {

namespace {

namespace type = takatori::type;

using step_set = std::multiset<takatori::plan::step const*>;

// Observations shared between a test and the injected extractor.
struct probe {
    std::size_t extractions{};
    std::vector<takatori::plan::step const*> queried{};
    std::weak_ptr<dist::operation_locations const> query{};
};

// Stands in for the formal operation location query until the compiler provides it.
class probing_locations final : public dist::operation_locations {
public:
    probing_locations(std::shared_ptr<probe> observed, location_id_set_type ids, bool fail_lookup, yugawara::compiler_result const* owner = nullptr, bool remote_read_only = false) :
        observed_(std::move(observed)),
        ids_(std::move(ids)),
        fail_lookup_(fail_lookup),
        owner_(owner),
        remote_read_only_(remote_read_only)
    {}

    location_id_set_type find(takatori::plan::step const& step) const override {
        if (owner_ != nullptr && !owner_->optional_statement()) {
            throw std::runtime_error("compiler result no longer owns the statement during location lookup");
        }
        observed_->queried.push_back(&step);
        if (fail_lookup_) throw std::runtime_error("lookup failed for test");
        if (remote_read_only_) {
            auto const& process = static_cast<takatori::plan::process const&>(step);
            for (auto const& op : process.operators()) {
                if (op.kind() == takatori::relation::expression_kind::scan ||
                    op.kind() == takatori::relation::expression_kind::find) return {7};
            }
            return {0};
        }
        return ids_;
    }

    std::string location_name(location_id_type id) const override {
        if (id == 7) return "worker";
        if (id == 8) return {};
        throw std::out_of_range("unknown location");
    }

private:
    std::shared_ptr<probe> observed_;
    location_id_set_type ids_;
    bool fail_lookup_;
    yugawara::compiler_result const* owner_{};
    bool remote_read_only_{};
};

// The extractor keeps no reference to the query it returns, as the compiler result owns it.
plan::compiler_context::operation_locations_extractor extractor(
    std::shared_ptr<probe> observed,
    dist::operation_locations::location_id_set_type ids,
    bool fail_lookup = false) {
    return [observed = std::move(observed), ids = std::move(ids), fail_lookup](yugawara::compiler_result const&)
        -> std::shared_ptr<dist::operation_locations const> {
        ++observed->extractions;
        auto query = std::make_shared<probing_locations const>(observed, ids, fail_lookup);
        observed->query = query;
        return query;
    };
}

class operation_location_compiler_test : public ::testing::Test {
protected:
    void SetUp() override {
        previous_ = global::config_pool();
        coordinator(true);
        previous_storage_ = global::storage_manager();
        auto manager = std::make_shared<storage::storage_manager>();
        ASSERT_TRUE(manager->add_entry(1, "T0"));
        global::storage_manager(std::move(manager));
        ctx_.storage_provider(tables());
        auto index = ctx_.storage_provider()->find_index("I0");
        ctx_.remote_catalogs(std::make_shared<dist::remote_catalog_snapshot>(
            dist::remote_catalog_snapshot::catalogs_type{}, dist::remote_catalog_snapshot::locations_type{},
            dist::remote_catalog_snapshot::definitions_type{{index.get(), {"worker", dist::remote_declaration_kind::index, 1257}}}));
    }

    void TearDown() override {
        if (previous_storage_) global::storage_manager(previous_storage_);
        if (previous_) global::config_pool(previous_);
    }

    static void coordinator(bool enabled) {
        auto cfg = std::make_shared<configuration>();
        cfg->distributed_query_coordinator_enabled(enabled);
        global::config_pool(cfg);
    }

    static std::shared_ptr<yugawara::storage::configurable_provider> tables() {
        auto storages = std::make_shared<yugawara::storage::configurable_provider>();
        auto t0 = storages->add_table({
            "T0",
            {
                {"C0", type::int8(), yugawara::variable::criteria{yugawara::variable::nullity{false}}},
                {"C1", type::float8()},
            },
        });
        storages->add_index({
            t0,
            "I0",
            {
                t0->columns()[0],
            },
            {
                t0->columns()[1],
            },
            {
                yugawara::storage::index_feature::find,
                yugawara::storage::index_feature::scan,
                yugawara::storage::index_feature::unique,
                yugawara::storage::index_feature::primary,
            },
        });
        return storages;
    }

    // Processes of the compiled graph, i.e. the identities the compiler would have placed.
    step_set compiled_processes() {
        step_set result{};
        auto& execute = dynamic_cast<takatori::statement::execute const&>(*ctx_.executable_statement()->statement());
        for (auto const& step : execute.execution_plan()) {
            if (step.kind() == takatori::plan::step_kind::process) result.insert(&step);
        }
        return result;
    }

    std::vector<executor::process::step*> runtime_processes() {
        std::vector<executor::process::step*> result{};
        auto& execute = dynamic_cast<executor::common::execute&>(*ctx_.executable_statement()->operators());
        for (auto const& step : execute.operators().steps()) {
            if (auto* process = dynamic_cast<executor::process::step*>(step.get())) result.emplace_back(process);
        }
        return result;
    }

    [[nodiscard]] std::string message() const {
        auto const& info = ctx_.error_info();
        return info ? std::string{info->message()} : std::string{};
    }

    takatori::util::maybe_shared_ptr<configuration> previous_{};
    std::shared_ptr<storage::storage_manager> previous_storage_{};
    plan::compiler_context ctx_{};
};

TEST_F(operation_location_compiler_test, missing_local_storage_is_not_treated_as_remote) {
    global::storage_manager()->clear();
    EXPECT_EQ(status::err_not_found, plan::prepare("SELECT C0 FROM T0", ctx_));
    ASSERT_TRUE(ctx_.error_info());
    EXPECT_EQ(error_code::target_not_found_exception, ctx_.error_info()->code());
}

TEST_F(operation_location_compiler_test, remote_writes_do_not_use_same_named_local_storage) {
    auto remote = tables();
    auto table = remote->find_table("T0");
    auto schemas = std::make_shared<yugawara::schema::configurable_provider>();
    schemas->add(yugawara::schema::declaration{std::nullopt, "public", remote});
    auto catalog = std::make_shared<yugawara::schema::catalog>("branch", std::nullopt, schemas);
    auto snapshot = std::make_shared<dist::remote_catalog_snapshot>(
        dist::remote_catalog_snapshot::catalogs_type{catalog},
        dist::remote_catalog_snapshot::locations_type{{table.get(), "worker"}});
    ASSERT_TRUE(global::storage_manager()->find_by_name("T0").has_value());
    struct sample { std::string_view sql; status result; error_code code; };
    for (auto const& test : {
        sample{"INSERT INTO T0(C0) VALUES(999)", status::err_illegal_operation, error_code::restricted_operation_exception},
        sample{"UPDATE T0 SET C1=1.0 WHERE C0=999", status::err_illegal_operation, error_code::restricted_operation_exception},
        sample{"DELETE FROM T0 WHERE C0=999", status::err_illegal_operation, error_code::restricted_operation_exception}}) {
        plan::compiler_context ctx{};
        ctx.storage_provider(remote);
        ctx.remote_catalogs(snapshot);
        // This injected analyzer input binds all three writes to the remote declaration.
        EXPECT_EQ(test.result, plan::prepare(test.sql, ctx)) << test.sql;
        ASSERT_TRUE(ctx.error_info());
        EXPECT_EQ(test.code, ctx.error_info()->code()) << test.sql;
    }
}

TEST_F(operation_location_compiler_test, location_lookup_precedes_statement_ownership_transfer) {
    auto observed = std::make_shared<probe>();
    ctx_.operation_locations([observed](yugawara::compiler_result const& result)
        -> std::shared_ptr<dist::operation_locations const> {
        return std::make_shared<probing_locations const>(observed,
            dist::operation_locations::location_id_set_type{0}, false, &result);
    });
    ASSERT_EQ(status::ok, plan::prepare("SELECT C0 FROM T0", ctx_)) << message();
    EXPECT_FALSE(observed->queried.empty());
    ASSERT_EQ(status::ok, plan::compile(ctx_)) << message();
    EXPECT_EQ(compiled_processes(), step_set(observed->queried.begin(), observed->queried.end()));
}

TEST_F(operation_location_compiler_test, all_local_locations_prepare_and_resolve_with_captured_placement) {
    auto observed = std::make_shared<probe>();
    ctx_.operation_locations(extractor(observed, {0}));

    ASSERT_EQ(status::ok, plan::prepare("SELECT C0 FROM T0", ctx_)) << message();
    EXPECT_EQ(1, observed->extractions);
    EXPECT_FALSE(observed->queried.empty());
    // The compiler-side query is gone once prepare has copied the placement.
    EXPECT_TRUE(observed->query.expired());

    ASSERT_EQ(status::ok, plan::compile(ctx_)) << message();
    // Every compiled process was queried exactly once, by its own identity.
    EXPECT_EQ(compiled_processes(), step_set(observed->queried.begin(), observed->queried.end()));
    auto processes = runtime_processes();
    ASSERT_FALSE(processes.empty());
    for (auto* process : processes) {
        ASSERT_TRUE(process);
        auto const& location = process->info()->captured_location();
        ASSERT_TRUE(location.has_value());
        EXPECT_TRUE(location->empty());
    }
    // Resolving uses the copy; neither extraction nor lookup is repeated.
    EXPECT_EQ(1, observed->extractions);
    EXPECT_EQ(compiled_processes().size(), observed->queried.size());
}

TEST_F(operation_location_compiler_test, explain_reports_captured_process_locations) {
    ctx_.operation_locations(extractor(std::make_shared<probe>(), {0}));
    ASSERT_EQ(status::ok, plan::prepare("SELECT C0 FROM T0", ctx_)) << message();
    ASSERT_EQ(status::ok, plan::compile(ctx_)) << message();

    api::impl::database database{};
    api::impl::executable_statement executable{ctx_.executable_statement(), {}, {}};
    std::ostringstream output{};
    ASSERT_EQ(status::ok, database.explain(executable, output));
    auto const json = nlohmann::json::parse(output.str());
    auto const& steps = json.at("execution_plan");
    std::size_t placed{};
    for (auto const& step : steps) {
        if (step.contains("location_id")) {
            EXPECT_EQ(0U, step.at("location_id"));
            EXPECT_EQ("local", step.at("location_name"));
            ++placed;
        }
    }
    EXPECT_GT(placed, 0U);
    EXPECT_EQ(compiled_processes().size(), placed);
}

TEST_F(operation_location_compiler_test, remote_location_on_inline_emit_is_rejected_by_formal_branch) {
    auto observed = std::make_shared<probe>();
    ctx_.operation_locations(extractor(observed, {7}));

    // The compiler emits scan -> emit in one process; the formal branch must not split it.
    EXPECT_EQ(status::err_unsupported, plan::prepare("SELECT C0 FROM T0", ctx_));
    EXPECT_EQ(1, observed->extractions);
    EXPECT_EQ(1, observed->queried.size());
    EXPECT_NE(std::string::npos, message().find("did not separate")) << message();
}

TEST_F(operation_location_compiler_test, ambiguous_or_missing_locations_are_generation_errors) {
    for (auto ids : {dist::operation_locations::location_id_set_type{}, {7, 8}, {9}, {8}}) {
        auto observed = std::make_shared<probe>();
        ctx_.operation_locations(extractor(observed, std::move(ids)));
        EXPECT_EQ(status::err_compiler_error, plan::prepare("SELECT C0 FROM T0", ctx_));
        ASSERT_TRUE(ctx_.error_info());
        EXPECT_EQ(error_code::compile_exception, ctx_.error_info()->code());
        EXPECT_EQ(status::err_compiler_error, ctx_.error_info()->status());
    }
}

TEST_F(operation_location_compiler_test, extractor_failure_is_reported_with_error_info) {
    ctx_.operation_locations([](yugawara::compiler_result const&) -> std::shared_ptr<dist::operation_locations const> {
        throw std::runtime_error("extractor unavailable");
    });

    EXPECT_EQ(status::err_compiler_error, plan::prepare("SELECT C0 FROM T0", ctx_));
    ASSERT_TRUE(ctx_.error_info());
    EXPECT_NE(std::string::npos, message().find("failed to obtain operation locations")) << message();
    EXPECT_NE(std::string::npos, message().find("extractor unavailable")) << message();
}

TEST_F(operation_location_compiler_test, lookup_failure_is_reported_with_error_info) {
    auto observed = std::make_shared<probe>();
    ctx_.operation_locations(extractor(observed, {0}, true));

    EXPECT_EQ(status::err_compiler_error, plan::prepare("SELECT C0 FROM T0", ctx_));
    EXPECT_EQ(1, observed->queried.size());
    ASSERT_TRUE(ctx_.error_info());
    EXPECT_NE(std::string::npos, message().find("operation location lookup failed")) << message();
    EXPECT_NE(std::string::npos, message().find("lookup failed for test")) << message();
}

TEST_F(operation_location_compiler_test, extractor_is_not_used_for_write_statements) {
    // Keep the successful local-write path covered independently of remote target rejection.
    ctx_.remote_catalogs(std::make_shared<dist::remote_catalog_snapshot>());
    auto observed = std::make_shared<probe>();
    ctx_.operation_locations(extractor(observed, {7}));

    ASSERT_EQ(status::ok, plan::prepare("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)", ctx_)) << message();
    EXPECT_EQ(0, observed->extractions);
    EXPECT_TRUE(observed->queried.empty());
}

TEST_F(operation_location_compiler_test, extractor_is_not_used_without_coordinator) {
    coordinator(false);
    auto observed = std::make_shared<probe>();
    ctx_.operation_locations(extractor(observed, {7}));

    ASSERT_EQ(status::ok, plan::prepare("SELECT C0 FROM T0", ctx_)) << message();
    ASSERT_EQ(status::ok, plan::compile(ctx_)) << message();
    EXPECT_EQ(0, observed->extractions);
    // No placement was captured, so runtime keeps its existing selection.
    for (auto* process : runtime_processes()) {
        ASSERT_TRUE(process);
        EXPECT_FALSE(process->info()->captured_location().has_value());
    }
}


} // namespace

TEST_F(operation_location_compiler_test, unsupported_remote_type_has_prepare_diagnostic) {
    auto observed = std::make_shared<probe>();
    ctx_.operation_locations([observed](yugawara::compiler_result const& result)
        -> std::shared_ptr<dist::operation_locations const> {
        return std::make_shared<probing_locations const>(observed,
            dist::operation_locations::location_id_set_type{0}, false, &result, true);
    });
    // Filter before LIMIT to keep the cast in the worker process.
    EXPECT_EQ(status::err_unsupported,
        plan::prepare("SELECT C0 FROM T0 WHERE CAST(C0 AS TINYINT) > 0 LIMIT 1", ctx_));
    ASSERT_TRUE(ctx_.error_info());
    EXPECT_NE(std::string::npos, message().find("unsupported remote execution type")) << message();
}

} // namespace jogasaki::testing

namespace jogasaki::testing {
namespace {
class native_location_compiler_test : public operation_location_compiler_test {
protected:
    void SetUp() override {
        operation_location_compiler_test::SetUp();
        auto remote = tables();
        auto table = remote->find_table("T0");
        auto index = remote->find_index("I0");
        auto schemas = std::make_shared<yugawara::schema::configurable_provider>();
        schemas->add(yugawara::schema::declaration{std::nullopt, "remote_schema", remote});
        auto catalog = std::make_shared<yugawara::schema::catalog>("branch", std::nullopt, schemas);
        auto snapshot = std::make_shared<dist::remote_catalog_snapshot>(
            dist::remote_catalog_snapshot::catalogs_type{catalog},
            dist::remote_catalog_snapshot::locations_type{{table.get(), "worker"}},
            dist::remote_catalog_snapshot::definitions_type{{index.get(), {"worker", dist::remote_declaration_kind::index, 1257}}});
        ctx_.remote_catalogs(snapshot);
        ctx_.symbol_locations(std::make_shared<dist::symbol_locations>(snapshot,
            std::make_shared<dist::location_registry>(), ctx_.storage_provider()));
    }
    std::vector<takatori::relation::expression_kind> remote_kinds(std::string_view sql) {
        if (plan::prepare(sql, ctx_) != status::ok) { ADD_FAILURE() << message(); return {}; }
        auto const& prepared = ctx_.prepared_statement();
        auto const& statement = static_cast<takatori::statement::execute const&>(*prepared->statement());
        std::vector<takatori::relation::expression_kind> kinds{};
        for (auto const& step : statement.execution_plan()) {
            if (step.kind() != takatori::plan::step_kind::process) continue;
            auto const& process = static_cast<takatori::plan::process const&>(step);
            auto* location = prepared->mirrors()->process_locations()->find(process);
            EXPECT_NE(nullptr, location);
            if (!location || location->server.empty()) continue;
            EXPECT_EQ("worker", location->server);
            takatori::relation::sort_from_upstream(process.operators(), [&](auto const& op) { kinds.push_back(op.kind()); });
        }
        return kinds;
    }
};

TEST_F(native_location_compiler_test, formal_three_part_name_pushes_project_and_filter) {
    using k = takatori::relation::expression_kind;
    EXPECT_EQ((std::vector<k>{k::scan, k::project, k::filter, k::offer}),
        remote_kinds("SELECT X FROM (SELECT C0 + 1 AS X FROM branch.remote_schema.T0) S WHERE X > 10"));
}

TEST_F(native_location_compiler_test, formal_key_condition_becomes_remote_find) {
    using k = takatori::relation::expression_kind;
    EXPECT_EQ((std::vector<k>{k::find, k::offer}),
        remote_kinds("SELECT C0 FROM branch.remote_schema.T0 WHERE C0=10"));
}

TEST_F(native_location_compiler_test, casts_stay_local_and_keep_supported_output_type) {
    using k = takatori::relation::expression_kind;
    EXPECT_EQ((std::vector<k>{k::scan, k::offer}),
        remote_kinds("SELECT C0 FROM branch.remote_schema.T0 WHERE CAST(C0 AS VARCHAR(20))='10'"));
}

TEST_F(native_location_compiler_test, local_only_sql_keeps_local_storage_and_location) {
    EXPECT_TRUE(remote_kinds("SELECT C0 FROM T0 WHERE C0=10").empty());
    ASSERT_EQ(status::ok, plan::compile(ctx_)) << message();
    for (auto* process : runtime_processes()) {
        ASSERT_TRUE(process->info()->captured_location());
        EXPECT_TRUE(process->info()->captured_location()->empty());
    }
}

TEST_F(native_location_compiler_test, catalog_omission_does_not_find_remote_schema) {
    EXPECT_EQ(status::err_compiler_error, plan::prepare("SELECT C0 FROM remote_schema.T0", ctx_));
    ASSERT_TRUE(ctx_.error_info());
    EXPECT_EQ(error_code::symbol_analyze_exception, ctx_.error_info()->code());
}

TEST_F(native_location_compiler_test, phase1_rejects_remote_writes_without_local_storage_fallback) {
    for (auto sql : {"INSERT INTO branch.remote_schema.T0(C0) VALUES(1)",
                     "UPDATE branch.remote_schema.T0 SET C1=1.0 WHERE C0=1",
                     "DELETE FROM branch.remote_schema.T0 WHERE C0=1"}) {
        EXPECT_EQ(status::err_compiler_error, plan::prepare(sql, ctx_)) << sql;
        ASSERT_TRUE(ctx_.error_info());
        EXPECT_EQ(error_code::unsupported_compiler_feature_exception, ctx_.error_info()->code()) << message();
        EXPECT_NE(std::string::npos, message().find("empty_operation_location")) << message();
    }
}
TEST_F(native_location_compiler_test, trailing_project_stays_on_coordinator) {
    using k = takatori::relation::expression_kind;
    EXPECT_EQ((std::vector<k>{k::scan, k::offer}),
        remote_kinds("SELECT C0+1 AS X FROM branch.remote_schema.T0"));
}

TEST_F(native_location_compiler_test, host_variable_stays_on_coordinator) {
    auto vars = std::make_shared<yugawara::variable::configurable_provider>();
    vars->add(yugawara::variable::declaration{"key", takatori::type::int8{}});
    ctx_.variable_provider(vars);
    ctx_.symbol_locations(std::make_shared<dist::symbol_locations>(ctx_.remote_catalogs(),
        std::make_shared<dist::location_registry>(), ctx_.storage_provider(),
        std::shared_ptr<yugawara::function::provider const>{},
        std::shared_ptr<yugawara::aggregate::provider const>{}, vars));
    using k = takatori::relation::expression_kind;
    EXPECT_EQ((std::vector<k>{k::scan, k::offer}),
        remote_kinds("SELECT C0 FROM branch.remote_schema.T0 WHERE C0=:key"));
}

TEST_F(native_location_compiler_test, unsupported_narrow_cast_does_not_enter_portable_ir) {
    using k = takatori::relation::expression_kind;
    for (auto sql : {"SELECT C0 FROM branch.remote_schema.T0 WHERE CAST(C0 AS TINYINT)>0",
                     "SELECT C0 FROM branch.remote_schema.T0 WHERE CAST(C0 AS SMALLINT)>0"}) {
        EXPECT_EQ((std::vector<k>{k::scan, k::offer}), remote_kinds(sql));
    }
}
TEST_F(native_location_compiler_test, missing_location_query_never_uses_local_or_fixture_fallback) {
    ctx_.operation_locations([](yugawara::compiler_result const&) -> std::shared_ptr<dist::operation_locations const> { return {}; });
    EXPECT_EQ(status::err_compiler_error, plan::prepare("SELECT C0 FROM branch.remote_schema.T0", ctx_));
    ASSERT_TRUE(ctx_.error_info());
    EXPECT_EQ(error_code::compile_exception, ctx_.error_info()->code());
    EXPECT_EQ("Compiler did not provide execution locations.", message());
}

TEST_F(native_location_compiler_test, missing_worker_definition_id_fails_before_execution) {
    auto catalogs = ctx_.remote_catalogs()->catalogs();
    auto table = catalogs.front()->schema_provider().find("remote_schema")->storage_provider().find_table("T0");
    ctx_.remote_catalogs(std::make_shared<dist::remote_catalog_snapshot>(catalogs,
        dist::remote_catalog_snapshot::locations_type{{table.get(), "worker"}}));
    EXPECT_EQ(status::err_compiler_error, plan::prepare("SELECT C0 FROM branch.remote_schema.T0", ctx_));
    ASSERT_TRUE(ctx_.error_info());
    EXPECT_NE(std::string::npos, message().find("remote identity")) << message();
}
} // namespace
} // namespace jogasaki::testing
