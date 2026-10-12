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
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
#include <gtest/gtest.h>

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
        // Without fixtures, the compiler has no distributed branch to exercise.
        if (!dist::distributed_query_fixtures_available()) GTEST_SKIP();
        unsetenv("JOGASAKI_DQ_REMOTE_EXECUTION_SERVER");
        previous_ = global::config_pool();
        coordinator(true);
        ctx_.storage_provider(tables());
    }

    void TearDown() override {
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
    plan::compiler_context ctx_{};
};

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

TEST_F(operation_location_compiler_test, remote_location_on_inline_emit_is_rejected_by_formal_branch) {
    auto observed = std::make_shared<probe>();
    ctx_.operation_locations(extractor(observed, {7}));

    // The compiler emits scan -> emit in one process; the formal branch must not split it.
    EXPECT_EQ(status::err_unsupported, plan::prepare("SELECT C0 FROM T0", ctx_));
    EXPECT_EQ(1, observed->extractions);
    EXPECT_EQ(1, observed->queried.size());
    EXPECT_NE(std::string::npos, message().find("did not separate")) << message();
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


// Dummy E3 consumes the engine's symbol query and produces locations for the actual compiler steps.
// It supports only a single scan/find source; it does not implement formal compiler placement.
class symbol_backed_locations final : public dist::operation_locations {
public:
    symbol_backed_locations(std::shared_ptr<dist::symbol_locations const> symbols,
        std::shared_ptr<std::vector<yugawara::storage::table const*>> observed)
        : symbols_(std::move(symbols)), observed_(std::move(observed)) {}
    location_id_set_type find(takatori::plan::step const& step) const override {
        if (step.kind() != takatori::plan::step_kind::process) return {0};
        auto const& process = static_cast<takatori::plan::process const&>(step);
        for (auto const& relation : process.operators()) {
            takatori::descriptor::relation const* source{};
            if (relation.kind() == takatori::relation::expression_kind::scan)
                source = &static_cast<takatori::relation::scan const&>(relation).source();
            if (relation.kind() == takatori::relation::expression_kind::find)
                source = &static_cast<takatori::relation::find const&>(relation).source();
            if (source) {
                auto const& index = yugawara::binding::extract<yugawara::storage::index>(*source);
                observed_->emplace_back(&index.table());
                return symbols_->find(index.table());
            }
        }
        return {0};
    }
    std::string location_name(location_id_type id) const override { return symbols_->of(id).name; }
private:
    std::shared_ptr<dist::symbol_locations const> symbols_{};
    std::shared_ptr<std::vector<yugawara::storage::table const*>> observed_{};
};

class symbol_location_compiler_test : public operation_location_compiler_test {};

TEST_F(symbol_location_compiler_test, local_compilation_uses_symbol_identity_for_operation_locations) {
    auto snapshot = std::make_shared<dist::remote_catalog_snapshot>();
    auto registry = std::make_shared<dist::location_registry>();
    auto symbols = std::make_shared<dist::symbol_locations>(snapshot, registry, ctx_.storage_provider());
    ctx_.remote_catalogs(snapshot);
    ctx_.symbol_locations(symbols);
    auto observed = std::make_shared<std::vector<yugawara::storage::table const*>>();
    ctx_.operation_locations([symbols, observed](yugawara::compiler_result const&) {
        return std::make_shared<symbol_backed_locations>(symbols, observed);
    });
    ASSERT_EQ(status::ok, plan::prepare("SELECT C0 FROM T0", ctx_)) << message();
    ASSERT_EQ(status::ok, plan::compile(ctx_)) << message();
    ASSERT_EQ(1U, observed->size());
    EXPECT_EQ(ctx_.storage_provider()->find_table("T0").get(), observed->front());
    for (auto* process : runtime_processes()) {
        ASSERT_TRUE(process->info()->captured_location());
        EXPECT_TRUE(process->info()->captured_location()->empty());
    }
}

TEST_F(symbol_location_compiler_test, remote_declaration_reaches_formal_graph_boundary_without_fixture_split) {
    auto remote_storage = tables();
    auto remote_table = remote_storage->find_table("T0");
    auto schemas = std::make_shared<yugawara::schema::configurable_provider>();
    schemas->add(yugawara::schema::declaration{std::nullopt, "remote_schema", remote_storage});
    auto catalog = std::make_shared<yugawara::schema::catalog>("remote_catalog", std::nullopt, schemas);
    auto snapshot = std::make_shared<dist::remote_catalog_snapshot>(
        dist::remote_catalog_snapshot::catalogs_type{catalog},
        dist::remote_catalog_snapshot::locations_type{{remote_table.get(), "worker"}});
    auto registry = std::make_shared<dist::location_registry>();
    auto symbols = std::make_shared<dist::symbol_locations>(snapshot, registry, ctx_.storage_provider());
    ctx_.remote_catalogs(snapshot);
    ctx_.symbol_locations(symbols);
    auto observed = std::make_shared<std::vector<yugawara::storage::table const*>>();
    ctx_.operation_locations([symbols, observed](yugawara::compiler_result const&) {
        return std::make_shared<symbol_backed_locations>(symbols, observed);
    });
    // Existing compiler leaves emit inline. The formal branch must reject it, not invent a new step.
    EXPECT_EQ(status::err_unsupported, plan::prepare("SELECT C0 FROM remote_schema.T0", ctx_));
    ASSERT_EQ(1U, observed->size());
    EXPECT_EQ(remote_table.get(), observed->front());
    EXPECT_EQ(catalog.get(), symbols->external_catalogs().front().get());
    EXPECT_NE(std::string::npos, message().find("did not separate the remote process")) << message();
}

} // namespace

TEST_F(operation_location_compiler_test, unsupported_remote_type_has_prepare_diagnostic) {
    auto observed = std::make_shared<probe>();
    ctx_.operation_locations([observed](yugawara::compiler_result const& result)
        -> std::shared_ptr<dist::operation_locations const> {
        return std::make_shared<probing_locations const>(observed,
            dist::operation_locations::location_id_set_type{0}, false, &result, true);
    });
    EXPECT_EQ(status::err_unsupported,
        plan::prepare("SELECT CAST(C0 AS TINYINT) FROM T0 LIMIT 1", ctx_));
    ASSERT_TRUE(ctx_.error_info());
    EXPECT_NE(std::string::npos, message().find("unsupported remote execution type")) << message();
}

} // namespace jogasaki::testing
