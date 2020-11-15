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
#pragma once

#include <cassert>

#include <takatori/relation/expression.h>
#include <takatori/util/fail.h>
#include <takatori/relation/step/offer.h>
#include <yugawara/compiled_info.h>

#include <yugawara/compiler_result.h>
#include <takatori/relation/graph.h>
#include <takatori/relation/scan.h>
#include <takatori/relation/emit.h>
#include <takatori/relation/step/dispatch.h>
#include <takatori/relation/expression.h>

#include <yugawara/binding/factory.h>
#include <yugawara/binding/extract.h>
#include <yugawara/binding/relation_info.h>

#include <jogasaki/data/small_record_store.h>
#include <jogasaki/utils/field_types.h>
#include <jogasaki/utils/relation_indices.h>
#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/executor/process/impl/ops/operator_base.h>
#include <jogasaki/executor/process/impl/ops/io_info.h>
#include <jogasaki/executor/process/impl/details/io_exchange_map.h>
#include <jogasaki/executor/process/impl/scan_info.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/executor/exchange/forward/step.h>
#include <jogasaki/executor/exchange/shuffle/step.h>
#include "operator_container.h"
#include "scan.h"
#include "emit.h"
#include "filter.h"
#include "project.h"
#include "take_group.h"
#include "offer.h"
#include "take_flat.h"

namespace jogasaki::executor::process::impl::ops {

namespace relation = takatori::relation;

using takatori::util::fail;
using takatori::relation::step::dispatch;
using takatori::util::maybe_shared_ptr;

/**
 * @brief generator for relational operators
 */
class operator_builder {
public:
    /**
     * @brief create empty object
     */
    operator_builder() = default;

    /**
     * @brief create new object
     * @param info the processor information
     * @param compiler_ctx compiler context
     * @param io_info I/O information
     * @param relation_io_map mapping from relation to I/O index
     * @param resource the memory resource used to building operators
     */
    operator_builder(
        std::shared_ptr<processor_info> info,
        plan::compiler_context const& compiler_ctx,
        std::shared_ptr<io_info> io_info,
        std::shared_ptr<relation_io_map> relation_io_map,
        memory::lifo_paged_memory_resource* resource = nullptr
    ) :
        info_(std::move(info)),
        compiler_ctx_(std::addressof(compiler_ctx)),
        io_info_(std::move(io_info)),
        relation_io_map_(std::move(relation_io_map)),
        resource_(resource)
    {}

    [[nodiscard]] operator_container operator()() && {
        auto root = dispatch(*this, head());
        return operator_container{std::move(root), index_, std::move(io_exchange_map_), std::move(scan_info_)};
    }

    [[nodiscard]] relation::expression& head() {
        relation::expression* result = nullptr;
        takatori::relation::enumerate_top(info_->relations(), [&](relation::expression& v) {
            result = &v;
        });
        if (result != nullptr) {
            return *result;
        }
        fail();
    }

    std::unique_ptr<operator_base> operator()(relation::find const& node) {
        (void)node;
        return {};
    }

    std::unique_ptr<operator_base> operator()(relation::scan const& node) {
        auto block_index = info_->scope_indices().at(&node);
        auto downstream = dispatch(*this, node.output().opposite()->owner());
        auto& index = yugawara::binding::extract<yugawara::storage::index>(node.source());

        // scan info is not passed to scan operator here, but passed back through task_context
        // in order to support parallel scan in the future
        scan_info_ = create_scan_info(node, index.keys());

        return std::make_unique<scan>(
            index_++,
            *info_,
            block_index,
            index.simple_name(),
            index,
            node.columns(),
            std::move(downstream)
        );
    }
    std::unique_ptr<operator_base> operator()(relation::join_find const& node) {
        (void)node;
        return {};
    }
    std::unique_ptr<operator_base> operator()(relation::join_scan const& node) {
        (void)node;
        return {};
    }
    std::unique_ptr<operator_base> operator()(relation::project const& node) {
        auto block_index = info_->scope_indices().at(&node);
        auto downstream = dispatch(*this, node.output().opposite()->owner());
        return std::make_unique<project>(index_++, *info_, block_index, node.columns(), std::move(downstream));
    }
    std::unique_ptr<operator_base> operator()(relation::filter const& node) {
        auto block_index = info_->scope_indices().at(&node);
        auto downstream = dispatch(*this, node.output().opposite()->owner());
        return std::make_unique<filter>(index_++, *info_, block_index, node.condition(), std::move(downstream));
    }
    std::unique_ptr<operator_base> operator()(relation::buffer const& node) {
        (void)node;
        return {};
    }

    std::unique_ptr<operator_base> operator()(relation::emit const& node) {
        auto block_index = info_->scope_indices().at(&node);
        auto e = std::make_unique<emit>(index_++, *info_, block_index, node.columns());
        auto writer_index = io_exchange_map_.add_external_output(e.get());
        e->external_writer_index(writer_index);
        return e;
    }

    std::unique_ptr<operator_base> operator()(relation::write const& node) {
        (void)node;
        return {};
    }
    std::unique_ptr<operator_base> operator()(relation::values const& node) {
        (void)node;
        return {};
    }
    std::unique_ptr<operator_base> operator()(relation::step::join const& node) {
        (void)node;
        return {};
    }
    std::unique_ptr<operator_base> operator()(relation::step::aggregate const& node) {
        (void)node;
        return {};
    }
    std::unique_ptr<operator_base> operator()(relation::step::intersection const& node) {
        (void)node;
        return {};
    }
    std::unique_ptr<operator_base> operator()(relation::step::difference const& node) {
        (void)node;
        return {};
    }
    std::unique_ptr<operator_base> operator()(relation::step::flatten const& node) {
        (void)node;
        return {};
    }

    std::unique_ptr<operator_base> operator()(relation::step::take_flat const& node) {
        auto block_index = info_->scope_indices().at(&node);
        auto reader_index = relation_io_map_->input_index(node.source());
        auto downstream = dispatch(*this, node.output().opposite()->owner());
        auto& input = io_info_->input_at(reader_index);
        assert(! input.is_group_input());  //NOLINT

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

    std::unique_ptr<operator_base> operator()(relation::step::take_group const& node) {
        auto block_index = info_->scope_indices().at(&node);
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

    std::unique_ptr<operator_base> operator()(relation::step::take_cogroup const& node) {
        (void)node;
        return {};
    }

    std::unique_ptr<operator_base> operator()(relation::step::offer const& node) {
        auto block_index = info_->scope_indices().at(&node);
        auto writer_index = relation_io_map_->output_index(node.destination());
        auto& output = io_info_->output_at(writer_index);
        return std::make_unique<offer>(index_++, *info_, block_index, output.column_order(), output.meta(), node.columns(), writer_index);
    }

    // keeping in public for testing
    using key = yugawara::storage::index::key;
    using endpoint = takatori::relation::scan::endpoint;
    std::shared_ptr<impl::scan_info> create_scan_info(
        endpoint const& lower,
        endpoint const& upper,
        std::vector<key, takatori::util::object_allocator<key>> const& index_keys
    ) {
        return std::make_shared<impl::scan_info>(
            encode_scan_endpoint(lower, index_keys),
            from(lower.kind()),
            encode_scan_endpoint(upper, index_keys),
            from(upper.kind())
        );
    }
    std::shared_ptr<impl::scan_info> create_scan_info(
        relation::scan const& node,
        std::vector<key, takatori::util::object_allocator<key>> const& index_keys
    ) {
        return create_scan_info(node.lower(), node.upper(), index_keys);
    }
private:
    std::shared_ptr<processor_info> info_{};
    plan::compiler_context const* compiler_ctx_{};
    std::shared_ptr<io_info> io_info_{};
    impl::details::io_exchange_map io_exchange_map_{};
    std::shared_ptr<relation_io_map> relation_io_map_{};
    operator_base::operator_index_type index_{};
    std::shared_ptr<impl::scan_info> scan_info_{};
    memory::lifo_paged_memory_resource* resource_{};

    kvs::end_point_kind from(relation::scan::endpoint::kind_type type) {
        using t = relation::scan::endpoint::kind_type;
        using k = kvs::end_point_kind;
        switch(type) {
            case t::unbound: return k::unbound;
            case t::inclusive: return k::inclusive;
            case t::exclusive: return k::exclusive;
            case t::prefixed_inclusive: return k::prefixed_inclusive;
            case t::prefixed_exclusive: return k::prefixed_exclusive;
        }
        fail();
    }

    std::string encode_scan_endpoint(
        relation::scan::endpoint const& e,
        std::vector<key, takatori::util::object_allocator<key>> const& index_keys
    ) {
        BOOST_ASSERT(e.keys().size() <= index_keys.size());  //NOLINT
        auto cp = resource_->get_checkpoint();
        executor::process::impl::block_scope scope{};
        std::string buf{};  //TODO create own buffer class
        for(int loop = 0; loop < 2; ++loop) { // first calculate buffer length, and then allocate/fill
            auto capacity = loop == 0 ? 0 : buf.capacity(); // capacity 0 makes stream empty write to calc. length
            kvs::stream s{buf.data(), capacity};
            std::size_t i = 0;
            for(auto&& k : e.keys()) {
                expression::evaluator eval{k.value(), info_->compiled_info()};
                auto res = eval(scope, resource_);
                auto odr = index_keys[i].direction() == relation::sort_direction::ascendant ?
                    kvs::order::ascending : kvs::order::descending;
                kvs::encode(res, utils::type_for(info_->compiled_info(), k.variable()), odr, s);
                resource_->deallocate_after(cp);
                ++i;
            }
            if (loop == 0) {
                buf.resize(s.length());
            }
        }
        return buf;
    }
};

/**
 * @brief create operators for a processor
 * @param info the processor information
 * @param compiler_ctx compiler context
 * @param io_info I/O information
 * @param relation_io_map mapping from relation to I/O index
 * @param resource the memory resource used to building operators
 * @return the container holding created operators and related information
 */
[[nodiscard]] inline operator_container create_operators(
    std::shared_ptr<processor_info> info,
    plan::compiler_context const& compiler_ctx,
    std::shared_ptr<io_info> io_info,
    std::shared_ptr<relation_io_map> relation_io_map,
    memory::lifo_paged_memory_resource* resource = nullptr
) {
    return operator_builder{std::move(info), compiler_ctx, std::move(io_info), std::move(relation_io_map), resource}();
}

}

