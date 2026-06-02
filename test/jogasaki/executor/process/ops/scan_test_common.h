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
#pragma once

#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <boost/container/container_fwd.hpp>
#include <boost/move/utility_core.hpp>
#include <gtest/gtest.h>

#include <takatori/descriptor/element.h>
#include <takatori/descriptor/variable.h>
#include <takatori/graph/graph.h>
#include <takatori/graph/port.h>
#include <takatori/plan/process.h>
#include <takatori/relation/endpoint_kind.h>
#include <takatori/relation/expression.h>
#include <takatori/relation/expression_kind.h>
#include <takatori/scalar/expression_kind.h>
#include <takatori/tree/tree_fragment_vector.h>
#include <takatori/util/clonable.h>
#include <takatori/util/sequence_view.h>

#include <yugawara/analyzer/expression_mapping.h>
#include <yugawara/analyzer/variable_resolution.h>
#include <yugawara/binding/factory.h>
#include <yugawara/storage/index.h>
#include <yugawara/storage/table.h>
#include <yugawara/variable/nullity.h>

#include <jogasaki/error/error_info.h>
#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/executor/process/impl/ops/operator_base.h>
#include <jogasaki/executor/process/impl/ops/operator_builder.h>
#include <jogasaki/executor/process/impl/ops/scan.h>
#include <jogasaki/executor/process/impl/ops/scan_context.h>
#include <jogasaki/executor/process/impl/scan_range.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/impl/variables_view.h>
#include <jogasaki/executor/process/io_exchange_map.h>
#include <jogasaki/executor/process/mock/task_context.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/kvs_test_base.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/operator_test_utils.h>
#include <jogasaki/test_root.h>
#include <jogasaki/test_utils.h>
#include <jogasaki/transaction_context.h>

namespace jogasaki::executor::process::impl::ops {

namespace relation = ::takatori::relation;
namespace scalar = ::takatori::scalar;

using yugawara::storage::table;
using variable = takatori::descriptor::variable;

/**
 * @brief Shared test fixture base for scan operator tests.
 *
 * @details Provides the scan_executor bundle, SetUp/TearDown lifecycle,
 *     add_scan_node helpers, make_scan_executor helpers, make_exprs,
 *     and make_scan_endpoint. Derive your GTest fixture from this class
 *     rather than replicating the infrastructure in each test file.
 */
class scan_test_base :
    public test_root,
    public kvs_test_base,
    public operator_test_utils {

public:
    /**
     * @brief Runtime bundle for a scan operator under test.
     *
     * @details range_ keeps the scan_range alive for task_ctx_ and ctx_.
     *     input_info_ and output_info_ are declared before op_ because op_ stores
     *     raw pointers into them. output_variables_ is declared before ctx_
     *     because ctx_ holds a reference to it. task_ctx_ is declared before
     *     ctx_ because ctx_ holds a pointer to it.
     *     Never copy or move: always create via scan_test_base::make_scan_executor().
     */
    struct scan_executor {
        std::shared_ptr<impl::scan_range> range_;
        variable_table_info input_info_{};
        variable_table_info output_info_;
        scan op_;
        variable_table_list variables_list_;
        mock::task_context task_ctx_;
        scan_context ctx_;

        scan_executor(
            std::shared_ptr<impl::scan_range> range_arg,
            variable_table_info out_info_arg,
            processor_info const& p_info,
            yugawara::storage::index const& primary_idx,
            sequence_view<relation::scan::column const> columns,
            yugawara::storage::index const* secondary_idx,
            std::unique_ptr<operator_base> downstream,
            std::unique_ptr<kvs::storage> primary_stg,
            std::unique_ptr<kvs::storage> secondary_stg,
            transaction_context* tx_raw,
            memory::lifo_paged_memory_resource* resource,
            memory::lifo_paged_memory_resource* varlen_resource
        ) :
            range_{std::move(range_arg)},
            output_info_{std::move(out_info_arg)},
            op_{0, p_info, 0, primary_idx, columns, secondary_idx,
                std::move(downstream), &input_info_, &output_info_},
            variables_list_{},
            task_ctx_{{}, {}, {}, range_},
            ctx_{&task_ctx_, variables_view{variables_list_, 0}, std::move(primary_stg), std::move(secondary_stg),
                tx_raw, range_.get(), resource, varlen_resource, nullptr}
        {
            variables_list_.emplace_back(output_info_);
        }

        scan_executor(scan_executor const&) = delete;
        scan_executor& operator=(scan_executor const&) = delete;
        scan_executor(scan_executor&&) = delete;
        scan_executor& operator=(scan_executor&&) = delete;
    };

    void SetUp() override {
        kvs_db_setup();
    }

    void TearDown() override {
        kvs_db_teardown();
    }

    /**
     * @brief Insert a scan relation node for the given table and index.
     *
     * @details All table columns are mapped to stream variables whose names match
     *     the column simple_name(). Column types are automatically bound in
     *     variable_map_. Optional lower and upper endpoints enable bounded scans.
     *
     * @param t      table whose columns define the scan outputs
     * @param idx    index to scan (primary or secondary)
     * @param lower  optional lower bound (default: unbound)
     * @param upper  optional upper bound (default: unbound)
     * @return reference to the newly inserted scan node
     */
    relation::scan& add_scan_node(
        std::shared_ptr<table> const& t,
        yugawara::storage::index const& idx,
        relation::scan::endpoint lower = {},
        relation::scan::endpoint upper = {}
    ) {
        std::vector<relation::scan::column> cols;
        for (auto const& col : t->columns()) {
            cols.emplace_back(bindings_(col), bindings_.stream_variable(col.simple_name()));
        }
        auto& target = process_.operators().insert(relation::scan{
            bindings_(idx), std::move(cols), std::move(lower), std::move(upper), {}
        });
        for (std::size_t i = 0; i < t->columns().size(); ++i) {
            yugawara::analyzer::variable_resolution r{
                takatori::util::clone_shared(t->columns()[i].type())};
            variable_map_->bind(target.columns()[i].source(), r, true);
            variable_map_->bind(target.columns()[i].destination(), r, true);
        }
        auto bind_key_expr_types = [&](auto const& keys) {
            for (auto const& k : keys) {
                for (std::size_t i = 0; i < t->columns().size(); ++i) {
                    if (bindings_(t->columns()[i]) == k.variable()) {
                        expression_map_->bind(
                            k.value(),
                            yugawara::analyzer::expression_resolution{
                                takatori::util::clone_shared(t->columns()[i].type())});
                        break;
                    }
                }
            }
        };
        bind_key_expr_types(target.lower().keys());
        bind_key_expr_types(target.upper().keys());
        return target;
    }

    /**
     * @brief Insert a scan node targeting the primary or secondary index of a table_setup.
     *
     * @param setup         table and index configuration
     * @param use_secondary if true, target setup.secondary_idx; else setup.primary_idx
     * @param lower         optional lower bound endpoint
     * @param upper         optional upper bound endpoint
     * @return reference to the newly inserted scan node
     */
    relation::scan& add_scan_node(
        table_setup const& setup,
        bool use_secondary = false,
        relation::scan::endpoint lower = {},
        relation::scan::endpoint upper = {}
    ) {
        yugawara::storage::index const& idx =
            use_secondary ? *setup.secondary_idx : *setup.primary_idx;
        return add_scan_node(setup.table, idx, std::move(lower), std::move(upper));
    }

    /**
     * @brief Wire the process graph, build processor_info, and return a scan_executor.
     *
     * @details Creates a dummy non-RTX transaction_context for operator_builder
     *     (which only uses it to determine scan parallelism mode). The real
     *     transaction tx is passed to scan_context for actual KVS access.
     *
     * @param target        the scan relation node
     * @param primary_idx   primary index (passed to scan operator)
     * @param secondary_idx optional secondary index, or nullptr
     * @param down          downstream verifier sink (take() is called here)
     * @param out_schema    basic_record defining the output variable layout
     * @param primary_stg   KVS storage for the primary index
     * @param secondary_stg KVS storage for the secondary index, or nullptr
     * @param tx            active transaction (used for scan_context)
     * @param host_vars     optional host variable table (passed to create_processor_info)
     * @return newly constructed scan_executor
     */
    scan_executor make_scan_executor(
        relation::scan& target,
        yugawara::storage::index const& primary_idx,
        yugawara::storage::index const* secondary_idx,
        record_verifier_sink& down,
        basic_record const& out_schema,
        std::unique_ptr<kvs::storage> primary_stg,
        std::unique_ptr<kvs::storage> secondary_stg,
        std::shared_ptr<transaction_context> tx,
        variable_table* host_vars = nullptr
    ) {
        target.output() >> down.input();
        create_processor_info(host_vars);
        auto dummy_tx = std::make_shared<transaction_context>();
        dummy_tx->error_info(create_error_info(error_code::none, "", status::err_unknown));
        request_context_.transaction(dummy_tx);
        variable_table_info out_info{
            create_variable_table_info(destinations(target.columns()), out_schema)};
        io_exchange_map exchange_map{};
        operator_builder builder{processor_info_, {}, {}, exchange_map, &request_context_};
        auto range = (builder.create_scan_ranges(target))[0];
        return scan_executor{
            range,
            std::move(out_info),
            *processor_info_,
            primary_idx,
            target.columns(),
            secondary_idx,
            down.take(),
            std::move(primary_stg),
            std::move(secondary_stg),
            tx.get(),
            request_context_.request_resource(),
            &varlen_resource_
        };
    }

    /**
     * @brief Wire graph, build executor, and open KVS storages from a table_setup.
     *
     * @param target        the scan relation node
     * @param setup         table and index configuration (supplies index names)
     * @param use_secondary if true, open and pass the secondary storage
     * @param down          downstream verifier sink
     * @param out_schema    output variable layout record
     * @param tx            active transaction
     * @param host_vars     optional host variable table
     * @return newly constructed scan_executor
     */
    scan_executor make_scan_executor(
        relation::scan& target,
        table_setup const& setup,
        bool use_secondary,
        record_verifier_sink& down,
        basic_record const& out_schema,
        std::shared_ptr<transaction_context> tx,
        variable_table* host_vars = nullptr
    ) {
        return make_scan_executor(
            target,
            *setup.primary_idx,
            use_secondary ? setup.secondary_idx.get() : nullptr,
            down,
            out_schema,
            get_storage(*db_, setup.primary_idx->simple_name()),
            use_secondary ? get_storage(*db_, setup.secondary_idx->simple_name()) : nullptr,
            std::move(tx),
            host_vars
        );
    }

    /**
     * @brief Construct a vector of key expressions from variadic unique_ptr arguments.
     */
    template<typename... Args>
    static std::vector<std::unique_ptr<scalar::expression>> make_exprs(Args&&... args) {
        std::vector<std::unique_ptr<scalar::expression>> v;
        (v.push_back(std::forward<Args>(args)), ...);
        return v;
    }

    /**
     * @brief Build a scan endpoint from column indices and expressions.
     *
     * @param setup       table and index setup (supplies column descriptors)
     * @param col_indices 0-based table column indices for the endpoint keys
     * @param exprs       one expression per column index (ownership transferred)
     * @param kind        endpoint kind
     * @return newly constructed scan endpoint
     */
    relation::scan::endpoint make_scan_endpoint(
        table_setup const& setup,
        std::vector<std::size_t> col_indices,
        std::vector<std::unique_ptr<scalar::expression>> exprs,
        relation::endpoint_kind kind
    ) {
        BOOST_ASSERT(col_indices.size() == exprs.size());  //NOLINT
        std::vector<relation::scan::key> keys;
        for (std::size_t i = 0; i < col_indices.size(); ++i) {
            keys.emplace_back(
                bindings_(setup.table->columns()[col_indices[i]]),
                std::move(exprs[i]));
        }
        return {std::move(keys), kind};
    }
};

} // namespace jogasaki::executor::process::impl::ops
