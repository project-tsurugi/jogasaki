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

#include <glog/logging.h>

#include <yugawara/compiler_result.h>
#include <takatori/relation/graph.h>
#include <takatori/relation/scan.h>
#include <takatori/relation/emit.h>
#include <takatori/relation/step/dispatch.h>

#include <jogasaki/meta/record_meta.h>
#include <jogasaki/data/record_store.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/executor/process/impl/ops/operator_container.h>
#include <jogasaki/executor/process/impl/work_context.h>
#include <jogasaki/executor/process/abstract/task_context.h>

namespace jogasaki::executor::process::impl::ops {

using namespace std::string_view_literals;
using namespace std::string_literals;

namespace graph = takatori::graph;
namespace relation = takatori::relation;

using takatori::util::fail;
using takatori::relation::step::dispatch;
using yugawara::compiled_info;

class output_verifier {
public:
    using find_verifier = std::function<bool(relation::find const&)>;
    using scan_verifier = std::function<bool(relation::scan const&)>;
    using join_find_verifier = std::function<bool(relation::join_find const&)>;
    using join_scan_verifier = std::function<bool(relation::join_scan const&)>;
    using project_verifier = std::function<bool(relation::project const&)>;
    using filter_verifier = std::function<bool(relation::filter const&)>;
    using buffer_verifier = std::function<bool(relation::buffer const&)>;
    using emit_verifier = std::function<bool(relation::emit const&)>;
    using write_verifier = std::function<bool(relation::write const&)>;
    using values_verifier = std::function<bool(relation::values const&)>;
    using join_verifier = std::function<bool(relation::step::join const&)>;
    using aggregate_verifier = std::function<bool(relation::step::aggregate const&)>;
    using intersection_verifier = std::function<bool(relation::step::intersection const&)>;
    using difference_verifier = std::function<bool(relation::step::difference const&)>;
    using flatten_verifier = std::function<bool(relation::step::flatten const&)>;
    using take_flat_verifier = std::function<bool(relation::step::take_flat const&)>;
    using take_group_verifier = std::function<bool(relation::step::take_group const&)>;
    using take_cogroup_verifier = std::function<bool(relation::step::take_cogroup const&)>;
    using offer_verifier = std::function<bool(relation::step::offer const&)>;

    output_verifier() = default;
    ~output_verifier() = default;
    output_verifier(output_verifier const& other) = delete;
    output_verifier& operator=(output_verifier const& other) = delete;
    output_verifier(output_verifier&& other) noexcept = delete;
    output_verifier& operator=(output_verifier&& other) noexcept = delete;

    explicit output_verifier(find_verifier verifier) noexcept : find_verifier_(std::move(verifier)) {}
    explicit output_verifier(scan_verifier verifier) noexcept : scan_verifier_(std::move(verifier)) {}
    explicit output_verifier(join_find_verifier verifier) noexcept : join_find_verifier_(std::move(verifier)) {}
    explicit output_verifier(join_scan_verifier verifier) noexcept : join_scan_verifier_(std::move(verifier)) {}
    explicit output_verifier(project_verifier verifier) noexcept : project_verifier_(std::move(verifier)) {}
    explicit output_verifier(filter_verifier verifier) noexcept : filter_verifier_(std::move(verifier)) {}
    explicit output_verifier(buffer_verifier verifier) noexcept : buffer_verifier_(std::move(verifier)) {}
    explicit output_verifier(emit_verifier verifier) noexcept : emit_verifier_(std::move(verifier)) {}
    explicit output_verifier(write_verifier verifier) noexcept : write_verifier_(std::move(verifier)) {}
    explicit output_verifier(values_verifier verifier) noexcept : values_verifier_(std::move(verifier)) {}
    explicit output_verifier(join_verifier verifier) noexcept : join_verifier_(std::move(verifier)) {}
    explicit output_verifier(aggregate_verifier verifier) noexcept : aggregate_verifier_(std::move(verifier)) {}
    explicit output_verifier(intersection_verifier verifier) noexcept : intersection_verifier_(std::move(verifier)) {}
    explicit output_verifier(difference_verifier verifier) noexcept : difference_verifier_(std::move(verifier)) {}
    explicit output_verifier(flatten_verifier verifier) noexcept : flatten_verifier_(std::move(verifier)) {}
    explicit output_verifier(take_flat_verifier verifier) noexcept : take_flat_verifier_(std::move(verifier)) {}
    explicit output_verifier(take_group_verifier verifier) noexcept : take_group_verifier_(std::move(verifier)) {}
    explicit output_verifier(take_cogroup_verifier verifier) noexcept : take_cogroup_verifier_(std::move(verifier)) {}
    explicit output_verifier(offer_verifier verifier) noexcept : offer_verifier_(std::move(verifier)) {}
    void verifier(flatten_verifier verifier) { flatten_verifier_ = std::move(verifier); }

    void operator()(relation::find const& node) { if (! find_verifier_ || ! find_verifier_(node)) throw std::runtime_error("failed"s); };
    void operator()(relation::scan const& node) { if (! scan_verifier_ || ! scan_verifier_(node)) throw std::runtime_error("failed"s); };
    void operator()(relation::join_find const& node) { if (! join_find_verifier_ || ! join_find_verifier_(node)) throw std::runtime_error("failed"s); };
    void operator()(relation::join_scan const& node) { if (! join_scan_verifier_ || ! join_scan_verifier_(node)) throw std::runtime_error("failed"s); };
    void operator()(relation::project const& node) { if (! project_verifier_ || ! project_verifier_(node)) throw std::runtime_error("failed"s); };
    void operator()(relation::filter const& node) { if (! filter_verifier_ || ! filter_verifier_(node)) throw std::runtime_error("failed"s); };
    void operator()(relation::buffer const& node) { if (! buffer_verifier_ || ! buffer_verifier_(node)) throw std::runtime_error("failed"s); };
    void operator()(relation::emit const& node) { if (! emit_verifier_ || ! emit_verifier_(node)) throw std::runtime_error("failed"s); };
    void operator()(relation::write const& node) { if (! write_verifier_ || ! write_verifier_(node)) throw std::runtime_error("failed"s); };
    void operator()(relation::values const& node) { if (! values_verifier_ || ! values_verifier_(node)) throw std::runtime_error("failed"s); };
    void operator()(relation::step::join const& node) { if (! join_verifier_ || ! join_verifier_(node)) throw std::runtime_error("failed"s); };
    void operator()(relation::step::aggregate const& node) { if (! aggregate_verifier_ || ! aggregate_verifier_(node)) throw std::runtime_error("failed"s); };
    void operator()(relation::step::intersection const& node) { if (! intersection_verifier_ || ! intersection_verifier_(node)) throw std::runtime_error("failed"s); };
    void operator()(relation::step::difference const& node) { if (! difference_verifier_ || ! difference_verifier_(node)) throw std::runtime_error("failed"s); };
    void operator()(relation::step::flatten const& node) { if (! flatten_verifier_ || ! flatten_verifier_(node)) throw std::runtime_error("failed"s); }
    void operator()(relation::step::take_flat const& node) { if (! take_flat_verifier_ || ! take_flat_verifier_(node)) throw std::runtime_error("failed"s); };
    void operator()(relation::step::take_group const& node) { if (! take_group_verifier_ || ! take_group_verifier_(node)) throw std::runtime_error("failed"s); };
    void operator()(relation::step::take_cogroup const& node) { if (! take_cogroup_verifier_ || ! take_cogroup_verifier_(node)) throw std::runtime_error("failed"s); };
    void operator()(relation::step::offer const& node) { if (! offer_verifier_ || ! offer_verifier_(node)) throw std::runtime_error("failed"s); };
    void operator()() {};

private:
    find_verifier find_verifier_;
    scan_verifier scan_verifier_;
    join_find_verifier join_find_verifier_;
    join_scan_verifier join_scan_verifier_;
    project_verifier project_verifier_;
    filter_verifier filter_verifier_;
    buffer_verifier buffer_verifier_;
    emit_verifier emit_verifier_;
    write_verifier write_verifier_;
    values_verifier values_verifier_;
    join_verifier join_verifier_;
    aggregate_verifier aggregate_verifier_;
    intersection_verifier intersection_verifier_;
    difference_verifier difference_verifier_;
    flatten_verifier flatten_verifier_;
    take_flat_verifier take_flat_verifier_;
    take_group_verifier take_group_verifier_;
    take_cogroup_verifier take_cogroup_verifier_;
    offer_verifier offer_verifier_;
};

}


