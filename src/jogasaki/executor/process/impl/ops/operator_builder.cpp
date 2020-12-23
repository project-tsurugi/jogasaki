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
#include "operator_builder.h"

#include <takatori/util/fail.h>

#include <takatori/relation/step/dispatch.h>
#include <yugawara/binding/factory.h>
#include <yugawara/binding/extract.h>
#include <yugawara/binding/relation_info.h>

#include "scan.h"
#include "emit.h"
#include "filter.h"
#include "project.h"
#include "take_group.h"
#include "take_cogroup.h"
#include "offer.h"
#include "take_flat.h"
#include "join.h"
#include "flatten.h"

namespace jogasaki::executor::process::impl::ops {

namespace relation = takatori::relation;

using takatori::util::fail;
using takatori::relation::step::dispatch;

operator_builder::operator_builder(std::shared_ptr<processor_info> info, const plan::compiler_context& compiler_ctx,
    std::shared_ptr<io_info> io_info, std::shared_ptr<relation_io_map> relation_io_map,
    io_exchange_map& io_exchange_map, memory::lifo_paged_memory_resource* resource) :
    info_(std::move(info)),
    compiler_ctx_(std::addressof(compiler_ctx)),
    io_info_(std::move(io_info)),
    io_exchange_map_(std::addressof(io_exchange_map)),
    relation_io_map_(std::move(relation_io_map)),
    resource_(resource)
{}

operator_container operator_builder::operator()()&& {
    auto root = dispatch(*this, head());
    return operator_container{std::move(root), index_, *io_exchange_map_, std::move(scan_info_)};
}

relation::expression& operator_builder::head() {
    relation::expression* result = nullptr;
    takatori::relation::enumerate_top(info_->relations(), [&](relation::expression& v) {
        result = &v;
    });
    if (result != nullptr) {
        return *result;
    }
    fail();
}

std::unique_ptr<operator_base> operator_builder::operator()(const relation::find& node) {
    (void)node;
    fail();
    return {};
}

std::unique_ptr<operator_base> operator_builder::operator()(const relation::scan& node) {
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

std::unique_ptr<operator_base> operator_builder::operator()(const relation::join_find& node) {
    (void)node;
    fail();
    return {};
}

std::unique_ptr<operator_base> operator_builder::operator()(const relation::project& node) {
    auto block_index = info_->scope_indices().at(&node);
    auto downstream = dispatch(*this, node.output().opposite()->owner());
    return std::make_unique<project>(index_++, *info_, block_index, node.columns(), std::move(downstream));
}

std::unique_ptr<operator_base> operator_builder::operator()(const relation::join_scan& node) {
    (void)node;
    fail();
    return {};
}

std::unique_ptr<operator_base> operator_builder::operator()(const relation::filter& node) {
    auto block_index = info_->scope_indices().at(&node);
    auto downstream = dispatch(*this, node.output().opposite()->owner());
    return std::make_unique<filter>(index_++, *info_, block_index, node.condition(), std::move(downstream));
}

std::unique_ptr<operator_base> operator_builder::operator()(const relation::buffer& node) {
    (void)node;
    fail();
    return {};
}

std::unique_ptr<operator_base> operator_builder::operator()(const relation::emit& node) {
    auto block_index = info_->scope_indices().at(&node);
    auto e = std::make_unique<emit>(index_++, *info_, block_index, node.columns());
    auto writer_index = io_exchange_map_->add_external_output(e.get());
    e->external_writer_index(writer_index);
    return e;
}

std::unique_ptr<operator_base> operator_builder::operator()(const relation::write& node) {
    (void)node;
    fail();
    return {};
}

std::unique_ptr<operator_base> operator_builder::operator()(const relation::values& node) {
    (void)node;
    fail();
    return {};
}

std::unique_ptr<operator_base> operator_builder::operator()(const relation::step::join& node) {
    auto block_index = info_->scope_indices().at(&node);
    auto downstream = dispatch(*this, node.output().opposite()->owner());
    return std::make_unique<join<data::iterable_record_store::iterator>>(index_++, *info_, block_index, node.operator_kind(), node.condition(), std::move(downstream));
}

std::unique_ptr<operator_base> operator_builder::operator()(const relation::step::aggregate& node) {
    (void)node;
    fail();
    return {};
}

std::unique_ptr<operator_base> operator_builder::operator()(const relation::step::intersection& node) {
    (void)node;
    fail();
    return {};
}

std::unique_ptr<operator_base> operator_builder::operator()(const relation::step::difference& node) {
    (void)node;
    fail();
    return {};
}

std::unique_ptr<operator_base> operator_builder::operator()(const relation::step::flatten& node) {
    auto block_index = info_->scope_indices().at(&node);
    auto downstream = dispatch(*this, node.output().opposite()->owner());
    return std::make_unique<flatten>(index_++, *info_, block_index, std::move(downstream));
}

std::unique_ptr<operator_base> operator_builder::operator()(const relation::step::take_flat& node) {
    auto block_index = info_->scope_indices().at(&node);
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

std::unique_ptr<operator_base> operator_builder::operator()(const relation::step::take_cogroup& node) {
    auto block_index = info_->scope_indices().at(&node);
    auto& block_info = info_->scopes_info()[block_index];
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
    auto block_index = info_->scope_indices().at(&node);
    auto writer_index = relation_io_map_->output_index(node.destination());
    auto& output = io_info_->output_at(writer_index);
    return std::make_unique<offer>(index_++, *info_, block_index, output.column_order(), output.meta(), node.columns(), writer_index);
}

std::shared_ptr<impl::scan_info>
operator_builder::create_scan_info(
    operator_builder::endpoint const& lower,
    operator_builder::endpoint const& upper,
    sequence_view<key const> index_keys
) {
    return std::make_shared<impl::scan_info>(
        encode_scan_endpoint(lower, index_keys),
        from(lower.kind()),
        encode_scan_endpoint(upper, index_keys),
        from(upper.kind())
    );
}

std::shared_ptr<impl::scan_info> operator_builder::create_scan_info(
    relation::scan const& node,
    sequence_view<key const> index_keys
) {
    return create_scan_info(node.lower(), node.upper(), index_keys);
}

kvs::end_point_kind operator_builder::from(relation::scan::endpoint::kind_type type) {
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

std::string operator_builder::encode_scan_endpoint(
    relation::scan::endpoint const& e,
    sequence_view<key const> index_keys
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
            auto spec = index_keys[i].direction() == relation::sort_direction::ascendant ?
                kvs::spec_key_ascending: kvs::spec_key_descending;
            kvs::encode(res, utils::type_for(info_->compiled_info(), k.variable()), spec, s);
            resource_->deallocate_after(cp);
            ++i;
        }
        if (loop == 0) {
            buf.resize(s.length());
        }
    }
    return buf;
}

operator_container create_operators(std::shared_ptr<processor_info> info, const plan::compiler_context& compiler_ctx,
    std::shared_ptr<io_info> io_info, std::shared_ptr<relation_io_map> relation_io_map,
    io_exchange_map& io_exchange_map, memory::lifo_paged_memory_resource* resource) {
    return operator_builder{
        std::move(info),
        compiler_ctx,
        std::move(io_info),
        std::move(relation_io_map),
        io_exchange_map,
        resource
    }();
}

}

