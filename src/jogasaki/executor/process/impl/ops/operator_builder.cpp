/*
 * Copyright 2018-2024 Project Tsurugi.
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
#include "operator_builder.h"

#include <cstddef>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <vector>
#include <boost/assert.hpp>

#include <takatori/descriptor/element.h>
#include <takatori/relation/graph.h>
#include <takatori/relation/step/dispatch.h>
#include <takatori/relation/write_kind.h>
#include <takatori/tree/tree_fragment_vector.h>
#include <takatori/util/exception.h>
#include <takatori/util/optional_ptr.h>
#include <takatori/util/reference_iterator.h>
#include <takatori/util/sequence_view.h>
#include <takatori/util/string_builder.h>
#include <yugawara/binding/extract.h>
#include <yugawara/storage/table.h>

#include <jogasaki/data/iterable_record_store.h>
#include <jogasaki/dist/key_range.h>
#include <jogasaki/dist/simple_key_distribution.h>
#include <jogasaki/dist/uniform_key_distribution.h>
#include <jogasaki/executor/process/impl/bound.h>
#include <jogasaki/executor/process/impl/ops/details/encode_key.h>
#include <jogasaki/executor/process/impl/ops/details/search_key_field_info.h>
#include <jogasaki/executor/process/impl/ops/io_info.h>
#include <jogasaki/executor/process/impl/ops/operator_base.h>
#include <jogasaki/executor/process/impl/ops/operator_container.h>
#include <jogasaki/executor/process/impl/scan_range.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/executor/process/io_exchange_map.h>
#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/executor/process/relation_io_map.h>
#include <jogasaki/executor/process/step.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/plan/plan_exception.h>
#include <jogasaki/utils/from_endpoint.h>
#include <jogasaki/utils/scan_parallel_enabled.h>

#include "aggregate_group.h"
#include "emit.h"
#include "filter.h"
#include "find.h"
#include "flatten.h"
#include "index_join.h"
#include "join.h"
#include "offer.h"
#include "project.h"
#include "scan.h"
#include "take_cogroup.h"
#include "take_flat.h"
#include "take_group.h"
#include "write_create.h"
#include "write_existing.h"
#include "write_kind.h"

namespace jogasaki::executor::process::impl::ops {

namespace relation = takatori::relation;

using takatori::relation::step::dispatch;
using takatori::util::string_builder;
using takatori::util::throw_exception;

operator_builder::operator_builder(
    std::shared_ptr<processor_info> info,
    std::shared_ptr<io_info> io_info,
    std::shared_ptr<relation_io_map> relation_io_map,
    io_exchange_map& io_exchange_map,
    request_context* request_context
) :
    info_(std::move(info)),
    io_info_(std::move(io_info)),
    io_exchange_map_(std::addressof(io_exchange_map)),
    relation_io_map_(std::move(relation_io_map)),
    request_context_(request_context)
{}

operator_container operator_builder::operator()()&& {
    auto root = dispatch(*this, head());
    return operator_container{std::move(root), index_, *io_exchange_map_, std::move(scan_ranges_)};
}

relation::expression const& operator_builder::head() {
    relation::expression const* result = nullptr;
    takatori::relation::enumerate_top(info_->relations(), [&](relation::expression const& v) {
        result = &v;
    });
    if (result != nullptr) {
        return *result;
    }
    throw_exception(std::logic_error{""});
}

std::unique_ptr<operator_base> operator_builder::operator()(const relation::find& node) {
    auto block_index = info_->block_indices().at(&node);
    auto downstream = dispatch(*this, node.output().opposite()->owner());
    auto& secondary_or_primary_index = yugawara::binding::extract<yugawara::storage::index>(node.source());
    auto& table = secondary_or_primary_index.table();
    auto primary = table.owner()->find_primary_index(table);
    BOOST_ASSERT(primary); //NOLINT
    return std::make_unique<find>(
        index_++,
        *info_,
        block_index,
        node.keys(),
        *primary,
        node.columns(),
        *primary != secondary_or_primary_index ? std::addressof(secondary_or_primary_index) : nullptr,
        std::move(downstream)
    );
}

std::unique_ptr<operator_base> operator_builder::operator()(const relation::scan& node) {
    auto block_index = info_->block_indices().at(&node);
    auto downstream = dispatch(*this, node.output().opposite()->owner());
    auto& secondary_or_primary_index = yugawara::binding::extract<yugawara::storage::index>(node.source());
    auto& table = secondary_or_primary_index.table();
    auto primary = table.owner()->find_primary_index(table);
    scan_ranges_ = create_scan_ranges(node);
    return std::make_unique<scan>(
        index_++,
        *info_,
        block_index,
        *primary,
        node.columns(),
        *primary != secondary_or_primary_index ? std::addressof(secondary_or_primary_index) : nullptr,
        std::move(downstream)
    );
}

std::unique_ptr<operator_base> operator_builder::operator()(const relation::join_find& node) {
    auto block_index = info_->block_indices().at(&node);
    auto downstream = dispatch(*this, node.output().opposite()->owner());
    auto& secondary_or_primary_index = yugawara::binding::extract<yugawara::storage::index>(node.source());
    auto& table = secondary_or_primary_index.table();
    auto primary = table.owner()->find_primary_index(table);
    return std::make_unique<join_find>(
        node.operator_kind(),
        index_++,
        *info_,
        block_index,
        *primary,
        node.columns(),
        node.keys(),
        node.condition(),
        *primary != secondary_or_primary_index ? std::addressof(secondary_or_primary_index) : nullptr,
        std::move(downstream)
    );
}

std::unique_ptr<operator_base> operator_builder::operator()(const relation::join_scan& node) {
    auto block_index = info_->block_indices().at(&node);
    auto downstream = dispatch(*this, node.output().opposite()->owner());
    auto& secondary_or_primary_index = yugawara::binding::extract<yugawara::storage::index>(node.source());
    auto& table = secondary_or_primary_index.table();
    auto primary = table.owner()->find_primary_index(table);
    return std::make_unique<join_scan>(
        node.operator_kind(),
        index_++,
        *info_,
        block_index,
        *primary,
        node.columns(),
        node.lower().keys(),
        utils::from(node.lower().kind()),
        node.upper().keys(),
        utils::from(node.upper().kind()),
        node.condition(),
        *primary != secondary_or_primary_index ? std::addressof(secondary_or_primary_index) : nullptr,
        std::move(downstream)
    );
}

std::unique_ptr<operator_base> operator_builder::operator()(const relation::project& node) {
    auto block_index = info_->block_indices().at(&node);
    auto downstream = dispatch(*this, node.output().opposite()->owner());
    return std::make_unique<project>(index_++, *info_, block_index, node.columns(), std::move(downstream));
}

std::unique_ptr<operator_base> operator_builder::operator()(const relation::filter& node) {
    auto block_index = info_->block_indices().at(&node);
    auto downstream = dispatch(*this, node.output().opposite()->owner());
    return std::make_unique<filter>(index_++, *info_, block_index, node.condition(), std::move(downstream));
}

std::unique_ptr<operator_base> operator_builder::operator()(const relation::buffer& node) {
    (void)node;
    throw_exception(std::logic_error{""});
    return {};
}

std::unique_ptr<operator_base> operator_builder::operator()(const relation::emit& node) {
    auto block_index = info_->block_indices().at(&node);
    auto e = std::make_unique<emit>(index_++, *info_, block_index, node.columns());
    io_exchange_map_->set_external_output(e.get());
    return e;
}

std::unique_ptr<operator_base> operator_builder::operator()(const relation::write& node) {
    auto block_index = info_->block_indices().at(&node);
    auto& index = yugawara::binding::extract<yugawara::storage::index>(node.destination());

    if (node.operator_kind() == relation::write_kind::update || node.operator_kind() == relation::write_kind::delete_) {
        return std::make_unique<write_existing>(
            index_++,
            *info_,
            block_index,
            write_kind_from(node.operator_kind()),
            index,
            node.keys(),
            node.columns()
        );
    }
    // INSERT from SELECT
    std::vector columns{node.keys()};
    columns.insert(columns.end(), node.columns().begin(), node.columns().end());
    return std::make_unique<write_create>(
        index_++,
        *info_,
        block_index,
        write_kind_from(node.operator_kind()),
        index,
        columns,
        request_context_->request_resource()
    );
}

std::unique_ptr<operator_base> operator_builder::operator()(const relation::values& node) {
    (void)node;
    throw_exception(std::logic_error{""});
    return {};
}

std::unique_ptr<operator_base> operator_builder::operator()(const relation::identify& node) {
    (void)node;
    throw_exception(std::logic_error{""});
    return {};
}

std::unique_ptr<operator_base> operator_builder::operator()(const relation::step::join& node) {
    auto block_index = info_->block_indices().at(&node);
    auto downstream = dispatch(*this, node.output().opposite()->owner());
    return std::make_unique<join<data::iterable_record_store::iterator>>(
        index_++,
        *info_,
        block_index,
        node.operator_kind(),
        node.condition(),
        std::move(downstream)
    );
}

std::unique_ptr<operator_base> operator_builder::operator()(const relation::step::aggregate& node) {
    auto block_index = info_->block_indices().at(&node);
    auto downstream = dispatch(*this, node.output().opposite()->owner());
    return std::make_unique<aggregate_group>(
        index_++,
        *info_,
        block_index,
        node.columns(),
        std::move(downstream)
    );
}

std::unique_ptr<operator_base> operator_builder::operator()(const relation::step::intersection& node) {
    (void)node;
    throw_exception(std::logic_error{""});
    return {};
}

std::unique_ptr<operator_base> operator_builder::operator()(const relation::step::difference& node) {
    (void)node;
    throw_exception(std::logic_error{""});
    return {};
}

std::unique_ptr<operator_base> operator_builder::operator()(const relation::step::flatten& node) {
    auto block_index = info_->block_indices().at(&node);
    auto downstream = dispatch(*this, node.output().opposite()->owner());
    return std::make_unique<flatten>(index_++, *info_, block_index, std::move(downstream));
}

std::unique_ptr<operator_base> operator_builder::operator()(const relation::step::take_flat& node) {
    auto block_index = info_->block_indices().at(&node);
    auto reader_index = relation_io_map_->input_index(node.source());
    auto downstream = dispatch(*this, node.output().opposite()->owner());
    auto& input = io_info_->input_at(reader_index);
    BOOST_ASSERT(! input.is_group_input());  //NOLINT

    return std::make_unique<take_flat>(
        index_++,
        *info_,
        block_index,
        input.column_order(),
        input.record_meta(),
        node.columns(),
        reader_index,
        std::move(downstream)
    );
}

std::unique_ptr<operator_base> operator_builder::operator()(const relation::step::take_group& node) {
    auto block_index = info_->block_indices().at(&node);
    auto reader_index = relation_io_map_->input_index(node.source());
    auto downstream = dispatch(*this, node.output().opposite()->owner());
    auto& input = io_info_->input_at(reader_index);
    return std::make_unique<take_group>(
        index_++,
        *info_,
        block_index,
        input.column_order(),
        input.group_meta(),
        node.columns(),
        reader_index,
        std::move(downstream)
    );
}

std::unique_ptr<operator_base> operator_builder::operator()(const relation::step::take_cogroup& node) {
    auto block_index = info_->block_indices().at(&node);
    auto& block_info = info_->vars_info_list()[block_index];
    std::vector<size_t> reader_indices{};
    std::vector<group_element> groups{};
    for(auto&& g : node.groups()) {
        auto reader_index = relation_io_map_->input_index(g.source());
        auto& input = io_info_->input_at(reader_index);
        groups.emplace_back(
            input.column_order(),
            input.group_meta(),
            g.columns(),
            reader_index,
            block_info
        );
    }
    auto downstream = dispatch(*this, node.output().opposite()->owner());
    return std::make_unique<take_cogroup>(
        index_++,
        *info_,
        block_index,
        std::move(groups),
        std::move(downstream)
    );
}

std::unique_ptr<operator_base> operator_builder::operator()(const relation::step::offer& node) {
    auto block_index = info_->block_indices().at(&node);
    auto writer_index = relation_io_map_->output_index(node.destination());
    auto& output = io_info_->output_at(writer_index);
    return std::make_unique<offer>(
        index_++,
        *info_,
        block_index,
        output.column_order(),
        output.meta(),
        node.columns(),
        writer_index
    );
}

std::vector<std::shared_ptr<impl::scan_range>> operator_builder::create_scan_ranges(relation::scan const& node) {
    std::vector<std::shared_ptr<impl::scan_range>> scan_ranges{};
    auto& secondary_or_primary_index =
        yugawara::binding::extract<yugawara::storage::index>(node.source());
    executor::process::impl::variable_table vars{};
    auto& table        = secondary_or_primary_index.table();
    auto primary       = table.owner()->find_primary_index(table);
    bool use_secondary = (*primary != secondary_or_primary_index);
    std::size_t blen{};
    std::size_t elen{};
    std::unique_ptr<data::aligned_buffer> key_begin = std::make_unique<data::aligned_buffer>();
    std::unique_ptr<data::aligned_buffer> key_end   = std::make_unique<data::aligned_buffer>();
    auto resource_ptr  = std::make_unique<ops::context_base::memory_resource>(&global::page_pool());
    auto status_result = details::two_encode_keys(request_context_,
        details::create_search_key_fields(secondary_or_primary_index, node.lower().keys(), *info_),
        details::create_search_key_fields(secondary_or_primary_index, node.upper().keys(), *info_),
        vars, *resource_ptr, *key_begin, blen, *key_end, elen);
    if (status_result != status::ok &&
        status_result != status::err_integrity_constraint_violation) {
        auto msg = string_builder{} << to_string_view(status_result) << string_builder::to_string;
        throw_exception(jogasaki::plan::plan_exception{create_error_info(
            error_code::sql_execution_exception, msg, status::err_compiler_error)});
    }
    auto begin_end_point_kind = kvs::adjust_endpoint_kind(use_secondary, utils::from(node.lower().kind()));
    auto end_end_point_kind   = kvs::adjust_endpoint_kind(use_secondary, utils::from(node.upper().kind()));
    bound begin(begin_end_point_kind, blen, std::move(key_begin));
    bound end(end_end_point_kind, elen, std::move(key_end));
    bool is_empty = (status_result == status::err_integrity_constraint_violation);

    auto [rtx_parallel_scan_enabled, scan_parallel_count] = utils::scan_parallel_enabled(*request_context_->transaction());
    const auto option = request_context_->transaction()->option();
    const bool is_rtx = option && option->readonly();
    if (rtx_parallel_scan_enabled && scan_parallel_count > 1 && is_rtx && !is_empty) {
        std::unique_ptr<kvs::storage> stg{};
        std::unique_ptr<dist::key_distribution> distribution{};
        if(global::config_pool()->key_distribution() == key_distribution_kind::uniform) {
            stg = request_context_->database()->get_storage(secondary_or_primary_index.simple_name());
            distribution = std::make_unique<dist::uniform_key_distribution>(
                *stg,
                *request_context_->transaction()->object(),
                request_context_
            );
        } else {
            distribution = std::make_unique<dist::simple_key_distribution>();
        }
        const jogasaki::dist::key_range range(
            begin.key(), begin.endpointkind(), end.key(), end.endpointkind());
        const auto pivot_count = scan_parallel_count - 1;
        auto pivots = distribution->compute_pivots(pivot_count, range); // possibly throws plan_exception
        scan_ranges.reserve(pivots.size() + 1);
        if (pivots.empty()) {
            scan_ranges.emplace_back(
                std::make_shared<impl::scan_range>(std::move(begin), std::move(end), is_empty));
        } else {
            // Add the initial scan ranges
            scan_ranges.emplace_back(std::make_shared<impl::scan_range>(std::move(begin),
                bound(kvs::end_point_kind::exclusive, pivots.front().size(),
                    std::make_unique<data::aligned_buffer>(pivots.front())),
                is_empty));
            // Add the intermediate scan ranges
            for (size_t i = 1; i < pivots.size(); ++i) {
                scan_ranges.emplace_back(std::make_shared<impl::scan_range>(
                    bound(kvs::end_point_kind::inclusive, pivots[i - 1].size(),
                        std::make_unique<data::aligned_buffer>(pivots[i - 1])),
                    bound(kvs::end_point_kind::exclusive, pivots[i].size(),
                        std::make_unique<data::aligned_buffer>(pivots[i])),
                    is_empty));
            }
            // Add the final scan ranges
            scan_ranges.emplace_back(std::make_shared<impl::scan_range>(
                bound(kvs::end_point_kind::inclusive, pivots.back().size(),
                    std::make_unique<data::aligned_buffer>(pivots.back())),
                std::move(end), is_empty));
        }
        VLOG_LP(log_trace) << "rtx scan runs in parallel:" << scan_ranges.size() << " config. max:" << scan_parallel_count;
    } else {
        scan_ranges.reserve(1);
        scan_ranges.emplace_back(
            std::make_shared<impl::scan_range>(std::move(begin), std::move(end), is_empty));
    }
    return scan_ranges;
}

operator_container create_operators(
    std::shared_ptr<processor_info> info,
    std::shared_ptr<io_info> io_info,
    std::shared_ptr<relation_io_map> relation_io_map,
    io_exchange_map& io_exchange_map,
    request_context* request_context
) {
    return operator_builder{
        std::move(info),
        std::move(io_info),
        std::move(relation_io_map),
        io_exchange_map,
        request_context
    }();
}

} // namespace jogasaki::executor::process::impl::ops
