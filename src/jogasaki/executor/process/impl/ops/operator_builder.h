/*
 * Copyright 2018-2023 Project Tsurugi.
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

#include <memory>

#include <takatori/relation/buffer.h>
#include <takatori/relation/emit.h>
#include <takatori/relation/expression.h>
#include <takatori/relation/expression_kind.h>
#include <takatori/relation/filter.h>
#include <takatori/relation/find.h>
#include <takatori/relation/graph.h>
#include <takatori/relation/identify.h>
#include <takatori/relation/join_find.h>
#include <takatori/relation/join_scan.h>
#include <takatori/relation/project.h>
#include <takatori/relation/scan.h>
#include <takatori/relation/step/aggregate.h>
#include <takatori/relation/step/difference.h>
#include <takatori/relation/step/flatten.h>
#include <takatori/relation/step/intersection.h>
#include <takatori/relation/step/join.h>
#include <takatori/relation/step/offer.h>
#include <takatori/relation/step/take_cogroup.h>
#include <takatori/relation/step/take_flat.h>
#include <takatori/relation/step/take_group.h>
#include <takatori/relation/values.h>
#include <takatori/relation/write.h>
#include <yugawara/binding/factory.h>
#include <yugawara/compiled_info.h>
#include <yugawara/compiler_result.h>
#include <yugawara/storage/index.h>

#include <jogasaki/executor/expr/evaluator.h>
#include <jogasaki/executor/process/impl/ops/io_info.h>
#include <jogasaki/executor/process/impl/ops/operator_base.h>
#include <jogasaki/executor/process/impl/scan_info.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/io_exchange_map.h>
#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/executor/process/relation_io_map.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/kvs/writable_stream.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/plan/compiler_context.h>
#include <jogasaki/utils/fail.h>

#include "details/search_key_field_info.h"
#include "operator_container.h"

namespace jogasaki::executor::process::impl::ops {

namespace relation = takatori::relation;

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
        std::shared_ptr<io_info> io_info,
        std::shared_ptr<relation_io_map> relation_io_map,
        io_exchange_map& io_exchange_map,
        memory::lifo_paged_memory_resource* resource = nullptr
    );

    [[nodiscard]] operator_container operator()() &&;

    [[nodiscard]] relation::expression const& head();

    std::unique_ptr<operator_base> operator()(relation::find const& node);
    std::unique_ptr<operator_base> operator()(relation::scan const& node);
    std::unique_ptr<operator_base> operator()(relation::join_find const& node);
    std::unique_ptr<operator_base> operator()(relation::join_scan const& node);
    std::unique_ptr<operator_base> operator()(relation::project const& node);
    std::unique_ptr<operator_base> operator()(relation::filter const& node);
    std::unique_ptr<operator_base> operator()(relation::buffer const& node);
    std::unique_ptr<operator_base> operator()(relation::emit const& node);
    std::unique_ptr<operator_base> operator()(relation::write const& node);
    std::unique_ptr<operator_base> operator()(relation::values const& node);
    std::unique_ptr<operator_base> operator()(relation::identify const& node);
    std::unique_ptr<operator_base> operator()(relation::step::join const& node);
    std::unique_ptr<operator_base> operator()(relation::step::aggregate const& node);
    std::unique_ptr<operator_base> operator()(relation::step::intersection const& node);
    std::unique_ptr<operator_base> operator()(relation::step::difference const& node);
    std::unique_ptr<operator_base> operator()(relation::step::flatten const& node);
    std::unique_ptr<operator_base> operator()(relation::step::take_flat const& node);
    std::unique_ptr<operator_base> operator()(relation::step::take_group const& node);
    std::unique_ptr<operator_base> operator()(relation::step::take_cogroup const& node);
    std::unique_ptr<operator_base> operator()(relation::step::offer const& node);

    // keeping in public for testing
    using key = yugawara::storage::index::key;
    using endpoint = takatori::relation::scan::endpoint;
    std::shared_ptr<impl::scan_info> create_scan_info(
        endpoint const& lower,
        endpoint const& upper,
        yugawara::storage::index const& index
    );

    std::shared_ptr<impl::scan_info> create_scan_info(
        relation::scan const& node,
        yugawara::storage::index const& index
    );


private:
    std::shared_ptr<processor_info> info_{};
    std::shared_ptr<io_info> io_info_{};
    io_exchange_map* io_exchange_map_{};
    std::shared_ptr<relation_io_map> relation_io_map_{};
    operator_base::operator_index_type index_{};
    std::shared_ptr<impl::scan_info> scan_info_{};
    memory::lifo_paged_memory_resource* resource_{};

    kvs::end_point_kind from(relation::scan::endpoint::kind_type type);

};

/**
 * @brief create operators for a processor
 * @param info the processor information
 * @param io_info I/O information
 * @param relation_io_map mapping from relation to I/O index
 * @param resource the memory resource used to building operators
 * @return the container holding created operators and related information
 * @throws plan::impl::compile_exception if the processor construction fails
 */
[[nodiscard]] operator_container create_operators(
    std::shared_ptr<processor_info> info,
    std::shared_ptr<io_info> io_info,
    std::shared_ptr<relation_io_map> relation_io_map,
    io_exchange_map& io_exchange_map,
    memory::lifo_paged_memory_resource* resource = nullptr
);

}

