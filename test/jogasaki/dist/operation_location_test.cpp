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


#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <gtest/gtest.h>

#include <takatori/plan/forward.h>
#include <takatori/plan/graph.h>
#include <takatori/plan/group.h>
#include <takatori/plan/process.h>
#include <takatori/plan/step_kind.h>
#include <takatori/relation/emit.h>
#include <takatori/relation/expression_kind.h>
#include <takatori/relation/graph.h>
#include <takatori/relation/project.h>
#include <takatori/relation/scan.h>
#include <takatori/relation/step/offer.h>
#include <takatori/relation/step/take_flat.h>
#include <takatori/scalar/cast.h>
#include <takatori/scalar/variable_reference.h>
#include <takatori/type/primitive.h>
#include <yugawara/analyzer/expression_mapping.h>
#include <yugawara/analyzer/variable_mapping.h>
#include <yugawara/binding/factory.h>
#include <yugawara/compiled_info.h>
#include <yugawara/storage/configurable_provider.h>
#include <yugawara/storage/index.h>
#include <yugawara/storage/table.h>

#include <jogasaki/dist/operation_location.h>
#include <jogasaki/dist/remote_catalog_provider.h>
#include <jogasaki/dist/remote_routing.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/plan/compiler.h>
#include <jogasaki/plan/mirror_container.h>
#include <jogasaki/plan/plan_exception.h>

namespace jogasaki::testing {

namespace {

namespace relation = takatori::relation;
namespace type = takatori::type;

using step_set = std::multiset<takatori::plan::step const*>;

// Stands in for the formal operation location query until the compiler provides it.
class dummy_operation_locations final : public dist::operation_locations {
public:
    using steps_type = std::unordered_map<takatori::plan::step const*, location_id_set_type>;
    using names_type = std::unordered_map<location_id_type, std::string>;

    dummy_operation_locations(steps_type steps, names_type names) :
        steps_(std::move(steps)),
        names_(std::move(names))
    {}

    location_id_set_type find(takatori::plan::step const& step) const override {
        queried_.push_back(&step);
        auto it = steps_.find(&step);
        // Throws rather than returning an empty set, so that looking up a step the compiler
        // never placed (foreign, or added after compilation) cannot pass silently.
        if (it == steps_.end()) throw std::out_of_range("step is not part of the compiled graph");
        return it->second;
    }

    std::string location_name(location_id_type id) const override {
        return names_.at(id);
    }

    [[nodiscard]] std::vector<takatori::plan::step const*> const& queried() const noexcept {
        return queried_;
    }

private:
    steps_type steps_;
    names_type names_;
    mutable std::vector<takatori::plan::step const*> queried_{};
};

class operation_location_test : public ::testing::Test {
protected:
    operation_location_test() {
        table_ = storages_.add_table({"T0", {{"C0", type::int8{}}}});
        index_ = storages_.add_index({table_, "I0"});
    }

    // Shape of a compiler which did not split the remote source from the local emit.
    takatori::plan::process& add_inline_process() {
        auto& process = plan_.insert(takatori::plan::process{});
        auto c0 = bindings_.stream_variable("c0");
        variables_->bind(c0, type::int8{});
        auto& scan = process.operators().insert(relation::scan{bindings_(*index_), {{bindings_(table_->columns()[0]), c0}}, {}, {}});
        auto& emit = process.operators().insert(relation::emit{
            {
                {c0, "c0"},
            },
        });
        scan.output() >> emit.input();
        return process;
    }

    // Shape after the compiler converted the fence into a forward exchange.
    std::pair<takatori::plan::process*, takatori::plan::process*> add_fenced_processes() {
        auto& source = plan_.insert(takatori::plan::process{});
        auto& forward = plan_.insert(takatori::plan::forward{bindings_.exchange_column("x0")});
        auto& consumer = plan_.insert(takatori::plan::process{});
        auto c0 = bindings_.stream_variable("c0");
        auto d0 = bindings_.stream_variable("d0");
        variables_->bind(c0, type::int8{});
        variables_->bind(forward.columns()[0], type::int8{});
        variables_->bind(d0, type::int8{});
        auto& scan = source.operators().insert(relation::scan{bindings_(*index_), {{bindings_(table_->columns()[0]), c0}}, {}, {}});
        auto& offer = source.operators().insert(relation::step::offer{bindings_.exchange(forward), {{c0, forward.columns()[0]}}});
        scan.output() >> offer.input();
        auto& take = consumer.operators().insert(relation::step::take_flat{bindings_.exchange(forward), {{forward.columns()[0], d0}}});
        auto& emit = consumer.operators().insert(relation::emit{
            {
                {d0, "c0"},
            },
        });
        take.output() >> emit.input();
        source >> forward;
        forward >> consumer;
        return {&source, &consumer};
    }

    [[nodiscard]] yugawara::compiled_info info() const {
        return {std::make_shared<yugawara::analyzer::expression_mapping>(), variables_};
    }

    [[nodiscard]] static std::vector<relation::expression_kind> kinds(takatori::plan::process const& process) {
        std::vector<relation::expression_kind> result{};
        relation::sort_from_upstream(process.operators(), [&](relation::expression const& op) {
            result.emplace_back(op.kind());
        });
        return result;
    }

    yugawara::binding::factory bindings_{};
    yugawara::storage::configurable_provider storages_{};
    std::shared_ptr<yugawara::storage::table const> table_{};
    std::shared_ptr<yugawara::storage::index const> index_{};
    std::shared_ptr<yugawara::analyzer::variable_mapping> variables_{std::make_shared<yugawara::analyzer::variable_mapping>()};
    takatori::plan::graph_type plan_{};
};

TEST_F(operation_location_test, queries_each_compiled_process_once_and_keeps_graph) {
    auto [source, consumer] = add_fenced_processes();
    auto source_kinds = kinds(*source);
    auto consumer_kinds = kinds(*consumer);
    dummy_operation_locations query{{{source, {7}}, {consumer, {0}}}, {{7, "worker"}}};

    auto captured = dist::capture_process_locations(plan_, query);

    ASSERT_TRUE(captured.error.empty()) << captured.error;
    // Exactly the compiled process objects, each once; the forward exchange is not placed.
    EXPECT_EQ((step_set{source, consumer}), step_set(query.queried().begin(), query.queried().end()));
    EXPECT_EQ(3, plan_.size());
    EXPECT_EQ(source_kinds, kinds(*source));
    EXPECT_EQ(consumer_kinds, kinds(*consumer));
    ASSERT_EQ(2, captured.locations->size());
    ASSERT_TRUE(captured.locations->find(*source));
    EXPECT_EQ("worker", captured.locations->find(*source)->server);
    // The local emit consumer stays on the coordinator.
    ASSERT_TRUE(captured.locations->find(*consumer));
    EXPECT_TRUE(captured.locations->find(*consumer)->server.empty());
}

TEST_F(operation_location_test, dummy_query_throws_for_steps_it_did_not_place) {
    auto [source, consumer] = add_fenced_processes();
    dummy_operation_locations query{{{source, {7}}, {consumer, {0}}}, {{7, "worker"}}};
    auto& added = plan_.insert(takatori::plan::process{});
    takatori::plan::process foreign{};

    EXPECT_THROW((void) query.find(added), std::out_of_range);
    EXPECT_THROW((void) query.find(foreign), std::out_of_range);
    EXPECT_THROW((void) query.location_name(9), std::out_of_range);
    // Capturing after a step was added reaches that step, which the query cannot answer.
    EXPECT_THROW((void) dist::capture_process_locations(plan_, query), std::out_of_range);
}

TEST_F(operation_location_test, captured_locations_reject_foreign_and_added_steps) {
    auto [source, consumer] = add_fenced_processes();
    dummy_operation_locations query{{{source, {7}}, {consumer, {0}}}, {{7, "worker"}}};
    auto captured = dist::capture_process_locations(plan_, query);
    ASSERT_TRUE(captured.error.empty()) << captured.error;
    auto& added = plan_.insert(takatori::plan::process{});
    takatori::plan::process foreign{};

    EXPECT_EQ(nullptr, captured.locations->find(added));
    EXPECT_EQ(nullptr, captured.locations->find(foreign));
    auto mirrors = std::make_shared<jogasaki::plan::mirror_container>();
    mirrors->process_locations(captured.locations);
    EXPECT_THROW((void) jogasaki::plan::impl::create(added, info(), mirrors, nullptr), jogasaki::plan::plan_exception);
    EXPECT_THROW((void) jogasaki::plan::impl::create(foreign, info(), mirrors, nullptr), jogasaki::plan::plan_exception);
    // Rejection uses the captured identity; the query is not consulted again.
    EXPECT_EQ(2, query.queried().size());
}

TEST_F(operation_location_test, captured_location_outlives_query_owner_and_reaches_processor) {
    auto [source, consumer] = add_fenced_processes();
    auto query = std::make_shared<dummy_operation_locations>(
        dummy_operation_locations::steps_type{{source, {7}}, {consumer, {0}}},
        dummy_operation_locations::names_type{{7, "worker"}});
    std::weak_ptr<dummy_operation_locations> observer = query;
    auto captured = dist::capture_process_locations(plan_, *query);
    ASSERT_TRUE(captured.error.empty()) << captured.error;
    query.reset();
    ASSERT_TRUE(observer.expired());

    auto compiled = info();
    auto mirrors = std::make_shared<jogasaki::plan::mirror_container>();
    mirrors->set(source, executor::process::impl::create_block_variables_definition(source->operators(), compiled));
    mirrors->set(consumer, executor::process::impl::create_block_variables_definition(consumer->operators(), compiled));
    mirrors->process_locations(captured.locations);
    auto remote = jogasaki::plan::impl::create(*source, compiled, mirrors, nullptr);
    auto local = jogasaki::plan::impl::create(*consumer, compiled, mirrors, nullptr);

    ASSERT_TRUE(remote.info()->captured_location());
    EXPECT_EQ("worker", *remote.info()->captured_location());
    ASSERT_TRUE(local.info()->captured_location());
    EXPECT_TRUE(local.info()->captured_location()->empty());
}

TEST_F(operation_location_test, rejects_remote_emit_consumer_without_rewriting) {
    auto [source, consumer] = add_fenced_processes();
    auto consumer_kinds = kinds(*consumer);
    dummy_operation_locations query{{{source, {0}}, {consumer, {7}}}, {{7, "worker"}}};

    auto captured = dist::capture_process_locations(plan_, query);

    EXPECT_NE(std::string::npos, captured.error.find("did not separate")) << captured.error;
    EXPECT_FALSE(captured.locations);
    EXPECT_EQ(3, plan_.size());
    EXPECT_EQ(consumer_kinds, kinds(*consumer));
}

TEST_F(operation_location_test, rejects_unsplit_remote_process_without_rewriting) {
    auto& process = add_inline_process();
    auto process_kinds = kinds(process);
    dummy_operation_locations query{{{&process, {7}}}, {{7, "worker"}}};

    auto captured = dist::capture_process_locations(plan_, query);

    EXPECT_NE(std::string::npos, captured.error.find("did not separate")) << captured.error;
    EXPECT_FALSE(captured.locations);
    EXPECT_EQ(1, plan_.size());
    EXPECT_EQ(process_kinds, kinds(process));
    EXPECT_EQ((step_set{&process}), step_set(query.queried().begin(), query.queried().end()));
}

TEST_F(operation_location_test, rejects_empty_multiple_and_unknown_locations) {
    auto [source, consumer] = add_fenced_processes();
    auto error_for = [&, source = source, consumer = consumer](dist::operation_locations::location_id_set_type ids) {
        dummy_operation_locations query{{{source, std::move(ids)}, {consumer, {0}}}, {{7, "worker"}, {8, "other"}}};
        return dist::capture_process_locations(plan_, query).error;
    };

    EXPECT_NE(std::string::npos, error_for({}).find("no executable location"));
    EXPECT_NE(std::string::npos, error_for({0, 7}).find("exactly one"));
    EXPECT_NE(std::string::npos, error_for({7, 8}).find("exactly one"));
    EXPECT_NE(std::string::npos, error_for({9}).find("unknown location ID 9"));
    EXPECT_TRUE(error_for({7}).empty());
}

TEST_F(operation_location_test, rejects_multiple_remote_processes) {
    auto [first, first_consumer] = add_fenced_processes();
    auto [second, second_consumer] = add_fenced_processes();
    dummy_operation_locations query{
        {{first, {7}}, {first_consumer, {0}}, {second, {7}}, {second_consumer, {0}}},
        {{7, "worker"}}};

    auto captured = dist::capture_process_locations(plan_, query);

    EXPECT_NE(std::string::npos, captured.error.find("at most one")) << captured.error;
    EXPECT_FALSE(captured.locations);
}

TEST_F(operation_location_test, rejects_remote_output_into_non_forward_exchange) {
    auto& process = plan_.insert(takatori::plan::process{});
    auto c0 = bindings_.stream_variable("c0");
    auto g0 = bindings_.exchange_column("g0");
    auto& group = plan_.insert(takatori::plan::group{
        {
            g0,
        },
        {
            g0,
        },
    });
    auto& scan = process.operators().insert(relation::scan{bindings_(*index_), {{bindings_(table_->columns()[0]), c0}}, {}, {}});
    auto& offer = process.operators().insert(relation::step::offer{bindings_.exchange(group), {{c0, g0}}});
    scan.output() >> offer.input();
    process >> group;
    dummy_operation_locations query{{{&process, {7}}}, {{7, "worker"}}};

    auto captured = dist::capture_process_locations(plan_, query);

    EXPECT_NE(std::string::npos, captured.error.find("single forward")) << captured.error;
    EXPECT_FALSE(captured.locations);
}

TEST_F(operation_location_test, rejects_remote_output_not_connected_downstream) {
    auto& process = plan_.insert(takatori::plan::process{});
    auto& forward = plan_.insert(takatori::plan::forward{bindings_.exchange_column("x0")});
    auto c0 = bindings_.stream_variable("c0");
    auto& scan = process.operators().insert(relation::scan{bindings_(*index_), {{bindings_(table_->columns()[0]), c0}}, {}, {}});
    auto& offer = process.operators().insert(relation::step::offer{bindings_.exchange(forward), {{c0, forward.columns()[0]}}});
    scan.output() >> offer.input();
    // The step graph has no process -> forward edge, so the runtime would bind no output.
    dummy_operation_locations query{{{&process, {7}}}, {{7, "worker"}}};

    auto captured = dist::capture_process_locations(plan_, query);

    EXPECT_NE(std::string::npos, captured.error.find("single forward")) << captured.error;
    EXPECT_FALSE(captured.locations);
}

TEST_F(operation_location_test, fixture_split_is_separate_from_operation_locations) {
    auto& process = add_inline_process();
    // A result computed for the graph as compiled, before any development split.
    dummy_operation_locations stale{{{&process, {7}}}, {{7, "worker"}}};
    dist::remote_catalog_snapshot catalogs{{}, {{table_.get(), "worker"}}};

    auto adapted = dist::adapt_remote_outputs(plan_, info(), &catalogs);
    (void) adapted;

    ASSERT_EQ(3, plan_.size());
    EXPECT_EQ(relation::expression_kind::offer, kinds(process).back());
    std::size_t consumers{};
    for (auto const& step : plan_) {
        if (step.kind() != takatori::plan::step_kind::process || &step == &process) continue;
        ++consumers;
        EXPECT_EQ((std::vector<relation::expression_kind>{relation::expression_kind::take_flat, relation::expression_kind::emit}),
            kinds(static_cast<takatori::plan::process const&>(step)));
    }
    EXPECT_EQ(1, consumers);
    EXPECT_TRUE(dist::validate_remote_plan(plan_, &catalogs).empty());
    // The consumer added by the split has no operation location in the earlier result.
    EXPECT_THROW((void) dist::capture_process_locations(plan_, stale), std::out_of_range);
}

} // namespace

TEST_F(operation_location_test, unsupported_type_is_a_diagnostic_not_a_query_exception) {
    auto [source, consumer] = add_fenced_processes();
    relation::scan* scan{};
    relation::step::offer* offer{};
    for (auto& op : source->operators()) {
        if (op.kind() == relation::expression_kind::scan) scan = &static_cast<relation::scan&>(op);
        if (op.kind() == relation::expression_kind::offer) offer = &static_cast<relation::step::offer&>(op);
    }
    ASSERT_TRUE(scan); ASSERT_TRUE(offer);
    auto c1 = bindings_.stream_variable("small");
    using scalar = takatori::scalar::cast;
    using column = relation::project::column;
    auto& project = source->operators().insert(relation::project{
        std::initializer_list<takatori::util::rvalue_reference_wrapper<column>>{
            column{c1, scalar{type::int1{}, takatori::scalar::cast_loss_policy::error,
                takatori::scalar::variable_reference{scan->columns()[0].destination()}}},
        },
    });
    scan->output().disconnect_all();
    scan->output() >> project.input();
    project.output() >> offer->input();
    dummy_operation_locations query{{{source, {7}}, {consumer, {0}}}, {{7, "worker"}}};
    dist::process_locations_result result{};
    EXPECT_NO_THROW(result = dist::capture_process_locations(plan_, query));
    EXPECT_FALSE(result.locations);
    EXPECT_NE(std::string::npos, result.error.find("unsupported remote execution type"));
}

} // namespace jogasaki::testing
