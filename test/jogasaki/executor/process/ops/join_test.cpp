/*
 * Copyright 2018-2025 Project Tsurugi.
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
#include <algorithm>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include <gtest/gtest.h>

#include <takatori/relation/step/join.h>
#include <takatori/scalar/unary.h>
#include <takatori/scalar/unary_operator.h>
#include <takatori/type/primitive.h>

#include <jogasaki/executor/process/impl/ops/cogroup.h>
#include <jogasaki/executor/process/impl/ops/join.h>
#include <jogasaki/executor/process/impl/ops/join_context.h>
#include <jogasaki/executor/process/impl/ops/operation_status.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/executor/process/impl/variables_view.h>
#include <jogasaki/executor/process/impl/work_context.h>
#include <jogasaki/executor/process/mock/iterable_group_store.h>
#include <jogasaki/executor/process/mock/task_context.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/memory/page_pool.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/group_meta.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/operator_test_utils.h>
#include <jogasaki/request_context.h>
#include <jogasaki/test_root.h>
#include <jogasaki/test_utils.h>
#include <jogasaki/utils/iterator_pair.h>

namespace jogasaki::executor::process::impl::ops {

using namespace meta;
using namespace testing;
using namespace executor;
using namespace accessor;
using namespace takatori::util;
using namespace jogasaki::mock;
using namespace std::string_view_literals;
using namespace std::string_literals;

using namespace jogasaki::memory;
using namespace boost::container::pmr;

namespace relation = ::takatori::relation;
namespace scalar = ::takatori::scalar;

using kind = meta::field_type_kind;
using group_reader = mock::basic_group_reader;
using group_type = group_reader::group_type;

class join_test : public test_root, public operator_test_utils {
public:
    using iterator = mock::iterable_group_store::iterator;
    using iterator_pair = utils::iterator_pair<iterator>;

    request_context request_context_{};

    /**
     * @brief Bundle of runtime objects needed to invoke and inspect the join operator.
     * @details op, variables, task_ctx, and ctx are stored together.
     *     ctx holds references into variables and task_ctx; the struct must not
     *     be move- or copy-constructed.  Always create via make_join_executor().
     */
    static variable_table_list make_variables_list(variable_table_info const& info) {
        variable_table_list list;
        list.emplace_back(info);
        return list;
    }

    struct join_executor {
        join<iterator> op_;
        variable_table_list variables_list_;
        variable_table& variables_;
        mock::task_context task_ctx_;
        join_context<iterator> ctx_;

        join_executor(
            join<iterator> op_arg,
            variable_table_info const& block_info,
            memory::lifo_paged_memory_resource* res,
            memory::lifo_paged_memory_resource* varlen_res,
            request_context* req_ctx
        ) :
            op_{std::move(op_arg)},
            variables_list_{make_variables_list(block_info)},
            variables_{variables_list_[0]},
            task_ctx_{{}, {}, {}, {}},
            ctx_{&task_ctx_, variables_view{variables_list_, op_.block_index()}, res, varlen_res}
        {
            ctx_.task_context().work_context(std::make_unique<impl::work_context>(
                req_ctx, 0, op_.block_index(), nullptr, nullptr, nullptr, nullptr, false, false
            ));
        }

        join_executor(join_executor const&) = delete;
        join_executor& operator=(join_executor const&) = delete;
        join_executor(join_executor&&) = delete;
        join_executor& operator=(join_executor&&) = delete;
    };

    /**
     * @brief Wire the process graph, build processor_info, construct the join operator,
     *     and return a join_executor.
     *
     * @details Uses C++17 guaranteed copy elision: the returned prvalue is constructed
     *     directly in the caller's variable, so the internal ctx references into
     *     variables and task_ctx remain valid.
     *
     * @param join_rel_op  the join relation node from emplace_operator
     * @param tc           the upstream take_cogroup node
     * @param down         downstream verifier sink (take() is called here)
     * @return newly constructed join_executor
     */
    join_executor make_join_executor(
        relation::step::join& join_rel_op,
        relation::step::take_cogroup& tc,
        record_verifier_sink& down
    ) {
        tc.output() >> join_rel_op.input();
        join_rel_op.output() >> down.input();
        create_processor_info(nullptr, true);
        join<iterator> op{0, *processor_info_, 0,
            join_rel_op.operator_kind(), join_rel_op.condition(), down.take()};
        auto const idx = op.block_index();
        return join_executor{std::move(op), processor_info_->vars_info_list()[idx],
            &resource_, &varlen_resource_, &request_context_};
    }

    /**
     * @brief Build the group_field mapping for one cogroup input group.
     *
     * Produces a group_field entry for each key column (is_key=true) followed by
     * each value column (is_key=false).  Source offsets come from the key/value
     * record_meta; target offsets come from the variable_table_info.
     *
     * @param block_info  variable_table_info for the operator's block (from join_executor::variables_.info())
     * @param grp_def     cogroup group definition carrying stream variables and metas
     * @return vector of group_field entries in key-first, then value order
     */
    std::vector<group_field> build_group_fields(
        variable_table_info const& block_info,
        cogroup_group_definition const& grp_def
    ) {
        std::vector<group_field> fields;
        auto const& key_meta = *grp_def.key_meta_;
        auto const& val_meta = *grp_def.value_meta_;
        for (std::size_t i = 0; i < grp_def.key_vars_.size(); ++i) {
            auto const& vi = block_info.at(grp_def.key_vars_[i]);
            fields.emplace_back(
                key_meta.at(i),
                key_meta.value_offset(i),
                vi.value_offset(),
                key_meta.nullity_offset(i),
                vi.nullity_offset(),
                key_meta.nullable(i),
                true  // is_key
            );
        }
        for (std::size_t i = 0; i < grp_def.value_vars_.size(); ++i) {
            auto const& vi = block_info.at(grp_def.value_vars_[i]);
            fields.emplace_back(
                val_meta.at(i),
                val_meta.value_offset(i),
                vi.value_offset(),
                val_meta.nullity_offset(i),
                vi.nullity_offset(),
                val_meta.nullable(i),
                false  // not is_key
            );
        }
        return fields;
    }

    /**
     * @brief Collect all output variables from all groups in order (key_vars then value_vars per group).
     */
    std::vector<descriptor::variable> all_output_vars(cogroup_input_definition const& in) {
        std::vector<descriptor::variable> vars;
        for (auto const& g : in) {
            for (auto const& v : g.key_vars_) vars.push_back(v);
            for (auto const& v : g.value_vars_) vars.push_back(v);
        }
        return vars;
    }

    /// @brief Per-group data specification for make_cogroup().
    struct group_store_spec {
        basic_record key;
        std::vector<basic_record> values;
    };

    /**
     * @brief Owns all runtime objects needed to exercise the join operator once.
     *
     * Holds the iterable_group_store objects (record data), per-group field mappings,
     * group_meta objects (for null-key checking), group<iterator> vector, and the
     * cogroup<iterator> in stable heap locations.
     * Call cgrp() to obtain a reference to the cogroup ready to pass to join_executor::op_().
     *
     * @note Non-copyable.  The default move constructor is safe: std::vector move
     *       transfers the internal heap buffer without relocating individual elements,
     *       so all sequence_views, iterators, and group_meta pointers stored in mygroups_
     *       and cgrp_ remain valid.
     */
    struct cogroup_run {
        std::vector<mock::iterable_group_store> stores_;
        std::vector<std::vector<group_field>>   all_fields_;
        std::vector<meta::group_meta>           key_metas_;
        std::vector<ops::group<iterator>>       mygroups_;
        cogroup<iterator>                       cgrp_;

        cogroup_run() = default;
        cogroup_run(cogroup_run const&) = delete;
        cogroup_run& operator=(cogroup_run const&) = delete;
        cogroup_run(cogroup_run&&) noexcept = default;
        cogroup_run& operator=(cogroup_run&&) noexcept = default;

        cogroup<iterator>& cgrp() noexcept { return cgrp_; }
    };

    /**
     * @brief Build a cogroup from inline record data.
     *
     * Creates one iterable_group_store per entry in @p specs, builds group field
     * mappings, and assembles the group<iterator> vector.  The returned cogroup_run
     * owns all objects; call cgrp() to pass the cogroup to the operator under test.
     *
     * @param ex     the join_executor providing the block_info
     * @param in     the cogroup_input_definition from add_upstream_cogroup_provider
     * @param specs  one {key, values} entry per input group, in the same order as @p in
     * @return a cogroup_run holding all required runtime objects
     */
    cogroup_run make_cogroup(
        join_executor& ex,
        cogroup_input_definition const& in,
        std::vector<group_store_spec> specs
    ) {
        cogroup_run run;
        auto& block_info = ex.variables_.info();
        for (auto const& grp_def : in) {
            run.all_fields_.push_back(build_group_fields(block_info, grp_def));
        }
        // Build group_meta objects before stores so pointers into key_metas_ are stable.
        run.key_metas_.reserve(in.size());
        for (auto const& grp_def : in) {
            run.key_metas_.emplace_back(grp_def.key_meta_, grp_def.value_meta_);
        }
        run.stores_.reserve(specs.size());
        for (auto& s : specs) {
            run.stores_.emplace_back(std::move(s.key), std::move(s.values));
        }
        run.mygroups_.reserve(run.stores_.size());
        for (std::size_t i = 0; i < run.stores_.size(); ++i) {
            auto& gs = run.stores_[i];
            run.mygroups_.emplace_back(
                iterator_pair{gs.begin(), gs.end()},
                run.all_fields_[i],
                gs.key().ref(),
                in[i].value_meta_->record_size()
            );
        }
        run.cgrp_ = cogroup<iterator>{run.mygroups_};
        return run;
    }
};

// -------------------------------------------------------------------------
// Tests
// -------------------------------------------------------------------------

TEST_F(join_test, simple) {
    // inner join: left {key=int4, value=int4, rows=[100,101]}
    //             right{key=int4, value=int4, rows=[200,201]}
    // → 2×2 = 4 joined rows
    auto key_rec = create_nullable_record<kind::int4>();
    auto val_rec = create_nullable_record<kind::int4>();

    auto [tc, in] = add_upstream_cogroup_provider({
        {key_rec.record_meta(), val_rec.record_meta()},
        {key_rec.record_meta(), val_rec.record_meta()}
    });

    auto out_vars = all_output_vars(in);
    auto& join_op = emplace_operator<relation::step::join>(relation::join_kind::inner);
    auto down = add_downstream_record_verifier(out_vars);
    auto ex = make_join_executor(join_op, tc, down);

    std::vector<basic_record> result{};
    down.set_body([&]() { result.push_back(get_variables(ex.variables_, out_vars)); });

    auto cg = make_cogroup(ex, in, {
        {create_nullable_record<kind::int4>(1), {
            create_nullable_record<kind::int4>(100),
            create_nullable_record<kind::int4>(101),
        }},
        {create_nullable_record<kind::int4>(1), {
            create_nullable_record<kind::int4>(200),
            create_nullable_record<kind::int4>(201),
        }},
    });
    auto st = ex.op_(ex.ctx_, cg.cgrp());
    ASSERT_EQ(operation_status_kind::ok, st.kind());

    ASSERT_EQ(4, result.size());
    std::vector<basic_record> exp{
        create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>(1,100,1,200),
        create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>(1,100,1,201),
        create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>(1,101,1,200),
        create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>(1,101,1,201),
    };
    std::sort(exp.begin(), exp.end());
    std::sort(result.begin(), result.end());
    ASSERT_EQ(exp, result);
    ex.ctx_.release();
}

TEST_F(join_test, multi_types) {
    // inner join: left {key=(int8,int4), value=(int8), rows=[100,101]}
    //             right{key=(int8,int4), value=(int8), rows=[200,201,202]}
    // → 2×3 = 6 joined rows
    auto key_rec = create_nullable_record<kind::int8, kind::int4>();
    auto val_rec = create_nullable_record<kind::int8>();

    auto [tc, in] = add_upstream_cogroup_provider({
        {key_rec.record_meta(), val_rec.record_meta()},
        {key_rec.record_meta(), val_rec.record_meta()}
    });

    auto out_vars = all_output_vars(in);
    auto& join_op = emplace_operator<relation::step::join>(relation::join_kind::inner);
    auto down = add_downstream_record_verifier(out_vars);
    auto ex = make_join_executor(join_op, tc, down);

    std::vector<basic_record> result{};
    down.set_body([&]() { result.push_back(get_variables(ex.variables_, out_vars)); });

    auto cg = make_cogroup(ex, in, {
        {create_nullable_record<kind::int8, kind::int4>(1, 10), {
            create_nullable_record<kind::int8>(100),
            create_nullable_record<kind::int8>(101),
        }},
        {create_nullable_record<kind::int8, kind::int4>(1, 10), {
            create_nullable_record<kind::int8>(200),
            create_nullable_record<kind::int8>(201),
            create_nullable_record<kind::int8>(202),
        }},
    });
    auto st = ex.op_(ex.ctx_, cg.cgrp());
    ASSERT_EQ(operation_status_kind::ok, st.kind());

    ASSERT_EQ(6, result.size());
    std::vector<basic_record> exp{
        create_nullable_record<kind::int8, kind::int4, kind::int8, kind::int8, kind::int4, kind::int8>(1,10,100,1,10,200),
        create_nullable_record<kind::int8, kind::int4, kind::int8, kind::int8, kind::int4, kind::int8>(1,10,100,1,10,201),
        create_nullable_record<kind::int8, kind::int4, kind::int8, kind::int8, kind::int4, kind::int8>(1,10,100,1,10,202),
        create_nullable_record<kind::int8, kind::int4, kind::int8, kind::int8, kind::int4, kind::int8>(1,10,101,1,10,200),
        create_nullable_record<kind::int8, kind::int4, kind::int8, kind::int8, kind::int4, kind::int8>(1,10,101,1,10,201),
        create_nullable_record<kind::int8, kind::int4, kind::int8, kind::int8, kind::int4, kind::int8>(1,10,101,1,10,202),
    };
    std::sort(exp.begin(), exp.end());
    std::sort(result.begin(), result.end());
    ASSERT_EQ(exp, result);
    ex.ctx_.release();
}

TEST_F(join_test, inner_join_empty_right) {
    // inner join: right group is empty → no output rows
    auto key_rec = create_nullable_record<kind::int4>();
    auto val_rec = create_nullable_record<kind::int4>();

    auto [tc, in] = add_upstream_cogroup_provider({
        {key_rec.record_meta(), val_rec.record_meta()},
        {key_rec.record_meta(), val_rec.record_meta()}
    });

    auto out_vars = all_output_vars(in);
    auto& join_op = emplace_operator<relation::step::join>(relation::join_kind::inner);
    auto down = add_downstream_record_verifier(out_vars);
    auto ex = make_join_executor(join_op, tc, down);

    std::vector<basic_record> result{};
    down.set_body([&]() { result.push_back(get_variables(ex.variables_, out_vars)); });

    auto cg = make_cogroup(ex, in, {
        {create_nullable_record<kind::int4>(1), {create_nullable_record<kind::int4>(100)}},
        {create_nullable_record<kind::int4>(1), {}},  // empty right group
    });
    auto st = ex.op_(ex.ctx_, cg.cgrp());
    ASSERT_EQ(operation_status_kind::ok, st.kind());

    ASSERT_EQ(0, result.size());
    ex.ctx_.release();
}

TEST_F(join_test, left_outer_join_no_match) {
    // left outer join: right group is empty → one output row with nulls for right columns
    auto key_rec = create_nullable_record<kind::int4>();
    auto val_rec = create_nullable_record<kind::int4>();

    auto [tc, in] = add_upstream_cogroup_provider({
        {key_rec.record_meta(), val_rec.record_meta()},
        {key_rec.record_meta(), val_rec.record_meta()}
    });

    auto out_vars = all_output_vars(in);
    auto& join_op = emplace_operator<relation::step::join>(relation::join_kind::left_outer);
    auto down = add_downstream_record_verifier(out_vars);
    auto ex = make_join_executor(join_op, tc, down);

    std::vector<basic_record> result{};
    down.set_body([&]() { result.push_back(get_variables(ex.variables_, out_vars)); });

    auto cg = make_cogroup(ex, in, {
        {create_nullable_record<kind::int4>(5), {create_nullable_record<kind::int4>(500)}},
        {create_nullable_record<kind::int4>(5), {}},  // empty right group
    });
    auto st = ex.op_(ex.ctx_, cg.cgrp());
    ASSERT_EQ(operation_status_kind::ok, st.kind());

    ASSERT_EQ(1, result.size());
    // Left columns: key=5, value=500; right columns: both null
    auto exp = create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>(
        5, 500, std::nullopt, std::nullopt);
    ASSERT_EQ(exp, result[0]);
    ex.ctx_.release();
}

TEST_F(join_test, left_join_with_condition) {
    // issue 583 – left outer join with IS NULL condition on right key.
    // Right group has rows (key=3 is not null), so IS NULL(right.key) is always false
    // for matched pairs → no inner output.  Left outer fallback emits one null-padded row.
    auto key_rec = create_nullable_record<kind::int4>();
    auto val_rec = create_nullable_record<kind::int4>();

    auto [tc, in] = add_upstream_cogroup_provider({
        {key_rec.record_meta(), val_rec.record_meta()},
        {key_rec.record_meta(), val_rec.record_meta()}
    });

    auto out_vars = all_output_vars(in);
    // condition: IS NULL(right key variable)
    auto& join_op = emplace_operator<relation::step::join>(
        relation::join_kind::left_outer,
        std::make_unique<scalar::unary>(scalar::unary_operator::is_null, varref(in[1].key(0)))
    );
    auto down = add_downstream_record_verifier(out_vars);
    auto ex = make_join_executor(join_op, tc, down);

    std::vector<basic_record> result{};
    down.set_body([&]() { result.push_back(get_variables(ex.variables_, out_vars)); });

    auto cg = make_cogroup(ex, in, {
        {create_nullable_record<kind::int4>(3), {create_nullable_record<kind::int4>(300)}},
        {create_nullable_record<kind::int4>(3), {
            create_nullable_record<kind::int4>(200),
            create_nullable_record<kind::int4>(201),
            create_nullable_record<kind::int4>(202),
        }},
    });
    auto st = ex.op_(ex.ctx_, cg.cgrp());
    ASSERT_EQ(operation_status_kind::ok, st.kind());

    // One null-padded left-outer row; right key and value must be null
    ASSERT_EQ(1, result.size());
    auto exp = create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>(
        3, 300, std::nullopt, std::nullopt);
    std::sort(result.begin(), result.end());
    ASSERT_EQ(exp, result[0]);
    ex.ctx_.release();
}

TEST_F(join_test, three_way_inner_join) {
    // inner join with 3 inputs: L{values=[10,11]} × M{values=[20,21]} × R{values=[30,31,32]}
    // → 2×2×3 = 12 joined rows
    auto key_rec = create_nullable_record<kind::int4>();
    auto val_rec = create_nullable_record<kind::int4>();

    auto [tc, in] = add_upstream_cogroup_provider({
        {key_rec.record_meta(), val_rec.record_meta()},
        {key_rec.record_meta(), val_rec.record_meta()},
        {key_rec.record_meta(), val_rec.record_meta()},
    });

    auto out_vars = all_output_vars(in);
    auto& join_op = emplace_operator<relation::step::join>(relation::join_kind::inner);
    auto down = add_downstream_record_verifier(out_vars);
    auto ex = make_join_executor(join_op, tc, down);

    std::vector<basic_record> result{};
    down.set_body([&]() { result.push_back(get_variables(ex.variables_, out_vars)); });

    auto cg = make_cogroup(ex, in, {
        {create_nullable_record<kind::int4>(1), {
            create_nullable_record<kind::int4>(10),
            create_nullable_record<kind::int4>(11),
        }},
        {create_nullable_record<kind::int4>(1), {
            create_nullable_record<kind::int4>(20),
            create_nullable_record<kind::int4>(21),
        }},
        {create_nullable_record<kind::int4>(1), {
            create_nullable_record<kind::int4>(30),
            create_nullable_record<kind::int4>(31),
            create_nullable_record<kind::int4>(32),
        }},
    });
    auto st = ex.op_(ex.ctx_, cg.cgrp());
    ASSERT_EQ(operation_status_kind::ok, st.kind());

    ASSERT_EQ(12, result.size());
    // Output column order: L_key, L_val, M_key, M_val, R_key, R_val
    std::vector<basic_record> exp;
    for (auto lv : {10, 11}) {
        for (auto mv : {20, 21}) {
            for (auto rv : {30, 31, 32}) {
                exp.push_back(create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4, kind::int4, kind::int4>(
                    1, lv, 1, mv, 1, rv));
            }
        }
    }
    std::sort(exp.begin(), exp.end());
    std::sort(result.begin(), result.end());
    ASSERT_EQ(exp, result);
    ex.ctx_.release();
}

TEST_F(join_test, semi_join_match) {
    // semi join: right has 2 rows both matching → left row emitted exactly once
    auto key_rec = create_nullable_record<kind::int4>();
    auto val_rec = create_nullable_record<kind::int4>();

    auto [tc, in] = add_upstream_cogroup_provider({
        {key_rec.record_meta(), val_rec.record_meta()},
        {key_rec.record_meta(), val_rec.record_meta()}
    });

    auto out_vars = all_output_vars(in);
    auto& join_op = emplace_operator<relation::step::join>(relation::join_kind::semi);
    auto down = add_downstream_record_verifier(out_vars);
    auto ex = make_join_executor(join_op, tc, down);

    std::vector<basic_record> result{};
    down.set_body([&]() { result.push_back(get_variables(ex.variables_, out_vars)); });

    auto cg = make_cogroup(ex, in, {
        {create_nullable_record<kind::int4>(1), {create_nullable_record<kind::int4>(100)}},
        {create_nullable_record<kind::int4>(1), {
            create_nullable_record<kind::int4>(200),
            create_nullable_record<kind::int4>(201),
        }},
    });
    auto st = ex.op_(ex.ctx_, cg.cgrp());
    ASSERT_EQ(operation_status_kind::ok, st.kind());

    ASSERT_EQ(1, result.size());
    // Semi join: left key=1, left val=100; right columns null
    auto exp = create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>(
        1, 100, std::nullopt, std::nullopt);
    ASSERT_EQ(exp, result[0]);
    ex.ctx_.release();
}

TEST_F(join_test, semi_join_empty_right) {
    // semi join: right group empty → no output
    auto key_rec = create_nullable_record<kind::int4>();
    auto val_rec = create_nullable_record<kind::int4>();

    auto [tc, in] = add_upstream_cogroup_provider({
        {key_rec.record_meta(), val_rec.record_meta()},
        {key_rec.record_meta(), val_rec.record_meta()}
    });

    auto out_vars = all_output_vars(in);
    auto& join_op = emplace_operator<relation::step::join>(relation::join_kind::semi);
    auto down = add_downstream_record_verifier(out_vars);
    auto ex = make_join_executor(join_op, tc, down);

    std::vector<basic_record> result{};
    down.set_body([&]() { result.push_back(get_variables(ex.variables_, out_vars)); });

    auto cg = make_cogroup(ex, in, {
        {create_nullable_record<kind::int4>(1), {create_nullable_record<kind::int4>(100)}},
        {create_nullable_record<kind::int4>(1), {}},  // empty right group
    });
    auto st = ex.op_(ex.ctx_, cg.cgrp());
    ASSERT_EQ(operation_status_kind::ok, st.kind());

    ASSERT_EQ(0, result.size());
    ex.ctx_.release();
}

TEST_F(join_test, semi_join_condition_no_match) {
    // semi join: condition IS NULL(right.key) is always false (key is non-null) → no output
    auto key_rec = create_nullable_record<kind::int4>();
    auto val_rec = create_nullable_record<kind::int4>();

    auto [tc, in] = add_upstream_cogroup_provider({
        {key_rec.record_meta(), val_rec.record_meta()},
        {key_rec.record_meta(), val_rec.record_meta()}
    });

    auto out_vars = all_output_vars(in);
    auto& join_op = emplace_operator<relation::step::join>(
        relation::join_kind::semi,
        std::make_unique<scalar::unary>(scalar::unary_operator::is_null, varref(in[1].key(0)))
    );
    auto down = add_downstream_record_verifier(out_vars);
    auto ex = make_join_executor(join_op, tc, down);

    std::vector<basic_record> result{};
    down.set_body([&]() { result.push_back(get_variables(ex.variables_, out_vars)); });

    auto cg = make_cogroup(ex, in, {
        {create_nullable_record<kind::int4>(1), {create_nullable_record<kind::int4>(100)}},
        {create_nullable_record<kind::int4>(1), {create_nullable_record<kind::int4>(200)}},
    });
    auto st = ex.op_(ex.ctx_, cg.cgrp());
    ASSERT_EQ(operation_status_kind::ok, st.kind());

    ASSERT_EQ(0, result.size());
    ex.ctx_.release();
}

TEST_F(join_test, anti_join_empty_right) {
    // anti join: right group empty → left row emitted with right columns null
    auto key_rec = create_nullable_record<kind::int4>();
    auto val_rec = create_nullable_record<kind::int4>();

    auto [tc, in] = add_upstream_cogroup_provider({
        {key_rec.record_meta(), val_rec.record_meta()},
        {key_rec.record_meta(), val_rec.record_meta()}
    });

    auto out_vars = all_output_vars(in);
    auto& join_op = emplace_operator<relation::step::join>(relation::join_kind::anti);
    auto down = add_downstream_record_verifier(out_vars);
    auto ex = make_join_executor(join_op, tc, down);

    std::vector<basic_record> result{};
    down.set_body([&]() { result.push_back(get_variables(ex.variables_, out_vars)); });

    auto cg = make_cogroup(ex, in, {
        {create_nullable_record<kind::int4>(1), {create_nullable_record<kind::int4>(100)}},
        {create_nullable_record<kind::int4>(1), {}},  // empty right group
    });
    auto st = ex.op_(ex.ctx_, cg.cgrp());
    ASSERT_EQ(operation_status_kind::ok, st.kind());

    ASSERT_EQ(1, result.size());
    auto exp = create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>(
        1, 100, std::nullopt, std::nullopt);
    ASSERT_EQ(exp, result[0]);
    ex.ctx_.release();
}

TEST_F(join_test, anti_join_condition_no_match) {
    // anti join: condition IS NULL(right.key) is always false → no match → left row emitted
    auto key_rec = create_nullable_record<kind::int4>();
    auto val_rec = create_nullable_record<kind::int4>();

    auto [tc, in] = add_upstream_cogroup_provider({
        {key_rec.record_meta(), val_rec.record_meta()},
        {key_rec.record_meta(), val_rec.record_meta()}
    });

    auto out_vars = all_output_vars(in);
    auto& join_op = emplace_operator<relation::step::join>(
        relation::join_kind::anti,
        std::make_unique<scalar::unary>(scalar::unary_operator::is_null, varref(in[1].key(0)))
    );
    auto down = add_downstream_record_verifier(out_vars);
    auto ex = make_join_executor(join_op, tc, down);

    std::vector<basic_record> result{};
    down.set_body([&]() { result.push_back(get_variables(ex.variables_, out_vars)); });

    auto cg = make_cogroup(ex, in, {
        {create_nullable_record<kind::int4>(1), {create_nullable_record<kind::int4>(100)}},
        {create_nullable_record<kind::int4>(1), {create_nullable_record<kind::int4>(200)}},
    });
    auto st = ex.op_(ex.ctx_, cg.cgrp());
    ASSERT_EQ(operation_status_kind::ok, st.kind());

    ASSERT_EQ(1, result.size());
    auto exp = create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>(
        1, 100, std::nullopt, std::nullopt);
    ASSERT_EQ(exp, result[0]);
    ex.ctx_.release();
}

TEST_F(join_test, left_outer_at_most_one_multiple_match) {
    // left_outer_at_most_one: two right rows match → operation aborted (scalar subquery violation)
    auto key_rec = create_nullable_record<kind::int4>();
    auto val_rec = create_nullable_record<kind::int4>();

    auto [tc, in] = add_upstream_cogroup_provider({
        {key_rec.record_meta(), val_rec.record_meta()},
        {key_rec.record_meta(), val_rec.record_meta()}
    });

    auto out_vars = all_output_vars(in);
    auto& join_op = emplace_operator<relation::step::join>(relation::join_kind::left_outer_at_most_one);
    auto down = add_downstream_record_verifier(out_vars);
    auto ex = make_join_executor(join_op, tc, down);

    std::vector<basic_record> result{};
    down.set_body([&]() { result.push_back(get_variables(ex.variables_, out_vars)); });

    auto cg = make_cogroup(ex, in, {
        {create_nullable_record<kind::int4>(1), {create_nullable_record<kind::int4>(100)}},
        {create_nullable_record<kind::int4>(1), {
            create_nullable_record<kind::int4>(200),
            create_nullable_record<kind::int4>(201),  // second match triggers abort
        }},
    });
    auto st = ex.op_(ex.ctx_, cg.cgrp());
    ASSERT_EQ(operation_status_kind::aborted, st.kind());
    // First match was emitted to downstream before the second match caused the abort
    ASSERT_EQ(1, result.size());
    ex.ctx_.release();
}

TEST_F(join_test, inner_join_condition_no_match) {
    // inner join: condition IS NULL(right.key) is always false (key is non-null) → no output
    auto key_rec = create_nullable_record<kind::int4>();
    auto val_rec = create_nullable_record<kind::int4>();

    auto [tc, in] = add_upstream_cogroup_provider({
        {key_rec.record_meta(), val_rec.record_meta()},
        {key_rec.record_meta(), val_rec.record_meta()}
    });

    auto out_vars = all_output_vars(in);
    auto& join_op = emplace_operator<relation::step::join>(
        relation::join_kind::inner,
        std::make_unique<scalar::unary>(scalar::unary_operator::is_null, varref(in[1].key(0)))
    );
    auto down = add_downstream_record_verifier(out_vars);
    auto ex = make_join_executor(join_op, tc, down);

    std::vector<basic_record> result{};
    down.set_body([&]() { result.push_back(get_variables(ex.variables_, out_vars)); });

    auto cg = make_cogroup(ex, in, {
        {create_nullable_record<kind::int4>(1), {create_nullable_record<kind::int4>(100)}},
        {create_nullable_record<kind::int4>(1), {create_nullable_record<kind::int4>(200)}},
    });
    auto st = ex.op_(ex.ctx_, cg.cgrp());
    ASSERT_EQ(operation_status_kind::ok, st.kind());

    ASSERT_EQ(0, result.size());
    ex.ctx_.release();
}

TEST_F(join_test, left_outer_join_left_empty) {
    // left outer join: left group is empty → no output
    auto key_rec = create_nullable_record<kind::int4>();
    auto val_rec = create_nullable_record<kind::int4>();

    auto [tc, in] = add_upstream_cogroup_provider({
        {key_rec.record_meta(), val_rec.record_meta()},
        {key_rec.record_meta(), val_rec.record_meta()}
    });

    auto out_vars = all_output_vars(in);
    auto& join_op = emplace_operator<relation::step::join>(relation::join_kind::left_outer);
    auto down = add_downstream_record_verifier(out_vars);
    auto ex = make_join_executor(join_op, tc, down);

    std::vector<basic_record> result{};
    down.set_body([&]() { result.push_back(get_variables(ex.variables_, out_vars)); });

    auto cg = make_cogroup(ex, in, {
        {create_nullable_record<kind::int4>(1), {}},  // empty left group
        {create_nullable_record<kind::int4>(1), {create_nullable_record<kind::int4>(200)}},
    });
    auto st = ex.op_(ex.ctx_, cg.cgrp());
    ASSERT_EQ(operation_status_kind::ok, st.kind());

    ASSERT_EQ(0, result.size());
    ex.ctx_.release();
}

TEST_F(join_test, semi_join_left_empty) {
    // semi join: left group is empty → no output
    auto key_rec = create_nullable_record<kind::int4>();
    auto val_rec = create_nullable_record<kind::int4>();

    auto [tc, in] = add_upstream_cogroup_provider({
        {key_rec.record_meta(), val_rec.record_meta()},
        {key_rec.record_meta(), val_rec.record_meta()}
    });

    auto out_vars = all_output_vars(in);
    auto& join_op = emplace_operator<relation::step::join>(relation::join_kind::semi);
    auto down = add_downstream_record_verifier(out_vars);
    auto ex = make_join_executor(join_op, tc, down);

    std::vector<basic_record> result{};
    down.set_body([&]() { result.push_back(get_variables(ex.variables_, out_vars)); });

    auto cg = make_cogroup(ex, in, {
        {create_nullable_record<kind::int4>(1), {}},  // empty left group
        {create_nullable_record<kind::int4>(1), {create_nullable_record<kind::int4>(200)}},
    });
    auto st = ex.op_(ex.ctx_, cg.cgrp());
    ASSERT_EQ(operation_status_kind::ok, st.kind());

    ASSERT_EQ(0, result.size());
    ex.ctx_.release();
}

TEST_F(join_test, anti_join_left_empty) {
    // anti join: left group is empty → no output
    auto key_rec = create_nullable_record<kind::int4>();
    auto val_rec = create_nullable_record<kind::int4>();

    auto [tc, in] = add_upstream_cogroup_provider({
        {key_rec.record_meta(), val_rec.record_meta()},
        {key_rec.record_meta(), val_rec.record_meta()}
    });

    auto out_vars = all_output_vars(in);
    auto& join_op = emplace_operator<relation::step::join>(relation::join_kind::anti);
    auto down = add_downstream_record_verifier(out_vars);
    auto ex = make_join_executor(join_op, tc, down);

    std::vector<basic_record> result{};
    down.set_body([&]() { result.push_back(get_variables(ex.variables_, out_vars)); });

    auto cg = make_cogroup(ex, in, {
        {create_nullable_record<kind::int4>(1), {}},  // empty left group
        {create_nullable_record<kind::int4>(1), {create_nullable_record<kind::int4>(200)}},
    });
    auto st = ex.op_(ex.ctx_, cg.cgrp());
    ASSERT_EQ(operation_status_kind::ok, st.kind());

    ASSERT_EQ(0, result.size());
    ex.ctx_.release();
}

TEST_F(join_test, full_outer_join_match) {
    // full outer join: both sides have rows → one joined row
    auto key_rec = create_nullable_record<kind::int4>();
    auto val_rec = create_nullable_record<kind::int4>();

    auto [tc, in] = add_upstream_cogroup_provider({
        {key_rec.record_meta(), val_rec.record_meta()},
        {key_rec.record_meta(), val_rec.record_meta()}
    });

    auto out_vars = all_output_vars(in);
    auto& join_op = emplace_operator<relation::step::join>(relation::join_kind::full_outer);
    auto down = add_downstream_record_verifier(out_vars);
    auto ex = make_join_executor(join_op, tc, down);

    std::vector<basic_record> result{};
    down.set_body([&]() { result.push_back(get_variables(ex.variables_, out_vars)); });

    auto cg = make_cogroup(ex, in, {
        {create_nullable_record<kind::int4>(1), {create_nullable_record<kind::int4>(100)}},
        {create_nullable_record<kind::int4>(1), {create_nullable_record<kind::int4>(200)}},
    });
    auto st = ex.op_(ex.ctx_, cg.cgrp());
    ASSERT_EQ(operation_status_kind::ok, st.kind());

    ASSERT_EQ(1, result.size());
    auto exp = create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>(1, 100, 1, 200);
    ASSERT_EQ(exp, result[0]);
    ex.ctx_.release();
}

TEST_F(join_test, full_outer_join_left_empty) {
    // full outer join: left group empty → right row emitted with nulls for left columns
    auto key_rec = create_nullable_record<kind::int4>();
    auto val_rec = create_nullable_record<kind::int4>();

    auto [tc, in] = add_upstream_cogroup_provider({
        {key_rec.record_meta(), val_rec.record_meta()},
        {key_rec.record_meta(), val_rec.record_meta()}
    });

    auto out_vars = all_output_vars(in);
    auto& join_op = emplace_operator<relation::step::join>(relation::join_kind::full_outer);
    auto down = add_downstream_record_verifier(out_vars);
    auto ex = make_join_executor(join_op, tc, down);

    std::vector<basic_record> result{};
    down.set_body([&]() { result.push_back(get_variables(ex.variables_, out_vars)); });

    auto cg = make_cogroup(ex, in, {
        {create_nullable_record<kind::int4>(1), {}},  // empty left group
        {create_nullable_record<kind::int4>(1), {create_nullable_record<kind::int4>(200)}},
    });
    auto st = ex.op_(ex.ctx_, cg.cgrp());
    ASSERT_EQ(operation_status_kind::ok, st.kind());

    ASSERT_EQ(1, result.size());
    auto exp = create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>(
        std::nullopt, std::nullopt, 1, 200);
    ASSERT_EQ(exp, result[0]);
    ex.ctx_.release();
}

TEST_F(join_test, full_outer_join_right_empty) {
    // full outer join: right group empty → left row emitted with nulls for right columns
    auto key_rec = create_nullable_record<kind::int4>();
    auto val_rec = create_nullable_record<kind::int4>();

    auto [tc, in] = add_upstream_cogroup_provider({
        {key_rec.record_meta(), val_rec.record_meta()},
        {key_rec.record_meta(), val_rec.record_meta()}
    });

    auto out_vars = all_output_vars(in);
    auto& join_op = emplace_operator<relation::step::join>(relation::join_kind::full_outer);
    auto down = add_downstream_record_verifier(out_vars);
    auto ex = make_join_executor(join_op, tc, down);

    std::vector<basic_record> result{};
    down.set_body([&]() { result.push_back(get_variables(ex.variables_, out_vars)); });

    auto cg = make_cogroup(ex, in, {
        {create_nullable_record<kind::int4>(1), {create_nullable_record<kind::int4>(100)}},
        {create_nullable_record<kind::int4>(1), {}},  // empty right group
    });
    auto st = ex.op_(ex.ctx_, cg.cgrp());
    ASSERT_EQ(operation_status_kind::ok, st.kind());

    ASSERT_EQ(1, result.size());
    auto exp = create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>(
        1, 100, std::nullopt, std::nullopt);
    ASSERT_EQ(exp, result[0]);
    ex.ctx_.release();
}

TEST_F(join_test, inner_join_null_key_no_condition) {
    // Baseline: inner join with null key and NO condition.
    // take_cogroup groups null records from both sides into the same cogroup (null-safe).
    // With no condition, every member pair matches → 1 output row.
    // This is non-standard SQL behavior; the compiler must inject a null filter to fix it.
    auto key_rec = create_nullable_record<kind::int4>();
    auto val_rec = create_nullable_record<kind::int4>();

    auto [tc, in] = add_upstream_cogroup_provider({
        {key_rec.record_meta(), val_rec.record_meta()},
        {key_rec.record_meta(), val_rec.record_meta()}
    });

    auto out_vars = all_output_vars(in);
    auto& join_op = emplace_operator<relation::step::join>(relation::join_kind::inner);
    auto down = add_downstream_record_verifier(out_vars);
    auto ex = make_join_executor(join_op, tc, down);

    std::vector<basic_record> result{};
    down.set_body([&]() { result.push_back(get_variables(ex.variables_, out_vars)); });

    auto cg = make_cogroup(ex, in, {
        {create_nullable_record<kind::int4>(std::nullopt), {create_nullable_record<kind::int4>(100)}},
        {create_nullable_record<kind::int4>(std::nullopt), {create_nullable_record<kind::int4>(200)}},
    });
    auto st = ex.op_(ex.ctx_, cg.cgrp());
    ASSERT_EQ(operation_status_kind::ok, st.kind());

    ASSERT_EQ(1, result.size());
    auto exp = create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>(
        std::nullopt, 100, std::nullopt, 200);
    ASSERT_EQ(exp, result[0]);
    ex.ctx_.release();
}

TEST_F(join_test, inner_join_null_key_null_filter_condition) {
    // Null filter via compiler-injected condition: inner join + null key + NOT IS_NULL(key).
    // take_cogroup still groups null records together, but the condition NOT IS_NULL(key)
    // evaluates to false for the null-key cogroup, so no member pair matches.
    // This implements SQL '=' (ternary/null-unsafe) semantics for the join key.
    auto key_rec = create_nullable_record<kind::int4>();
    auto val_rec = create_nullable_record<kind::int4>();

    auto [tc, in] = add_upstream_cogroup_provider({
        {key_rec.record_meta(), val_rec.record_meta()},
        {key_rec.record_meta(), val_rec.record_meta()}
    });

    auto out_vars = all_output_vars(in);
    auto& join_op = emplace_operator<relation::step::join>(
        relation::join_kind::inner,
        std::make_unique<scalar::unary>(
            scalar::unary_operator::conditional_not,
            scalar::unary{scalar::unary_operator::is_null, varref(in[0].key(0))}
        )
    );
    auto down = add_downstream_record_verifier(out_vars);
    auto ex = make_join_executor(join_op, tc, down);

    std::vector<basic_record> result{};
    down.set_body([&]() { result.push_back(get_variables(ex.variables_, out_vars)); });

    auto cg = make_cogroup(ex, in, {
        {create_nullable_record<kind::int4>(std::nullopt), {create_nullable_record<kind::int4>(100)}},
        {create_nullable_record<kind::int4>(std::nullopt), {create_nullable_record<kind::int4>(200)}},
    });
    auto st = ex.op_(ex.ctx_, cg.cgrp());
    ASSERT_EQ(operation_status_kind::ok, st.kind());

    ASSERT_EQ(0, result.size());
    ex.ctx_.release();
}

TEST_F(join_test, anti_join_null_key) {
    // take_cogroup groups null records together; anti join finds G2 non-empty
    // and condition absent (= true), so G1 member is NOT emitted.
    auto key_rec = create_nullable_record<kind::int4>();
    auto val_rec = create_nullable_record<kind::int4>();

    auto [tc, in] = add_upstream_cogroup_provider({
        {key_rec.record_meta(), val_rec.record_meta()},
        {key_rec.record_meta(), val_rec.record_meta()}
    });

    auto out_vars = all_output_vars(in);
    auto& join_op = emplace_operator<relation::step::join>(relation::join_kind::anti);
    auto down = add_downstream_record_verifier(out_vars);
    auto ex = make_join_executor(join_op, tc, down);

    std::vector<basic_record> result{};
    down.set_body([&]() { result.push_back(get_variables(ex.variables_, out_vars)); });

    auto cg = make_cogroup(ex, in, {
        {create_nullable_record<kind::int4>(std::nullopt), {create_nullable_record<kind::int4>(100)}},
        {create_nullable_record<kind::int4>(std::nullopt), {create_nullable_record<kind::int4>(200)}},
    });
    auto st = ex.op_(ex.ctx_, cg.cgrp());
    ASSERT_EQ(operation_status_kind::ok, st.kind());

    // null <=> null → matched → G1 row suppressed
    ASSERT_EQ(0, result.size());
    ex.ctx_.release();
}

}
