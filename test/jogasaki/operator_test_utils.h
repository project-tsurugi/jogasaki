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

#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <gtest/gtest.h>

#include <takatori/plan/forward.h>
#include <takatori/plan/group.h>
#include <takatori/relation/step/offer.h>
#include <takatori/relation/step/take_flat.h>
#include <takatori/relation/step/take_group.h>
#include <takatori/type/character.h>
#include <takatori/type/date.h>
#include <takatori/type/decimal.h>
#include <takatori/type/int.h>
#include <takatori/type/time_of_day.h>
#include <takatori/type/time_point.h>
#include <takatori/value/character.h>
#include <takatori/value/int.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <yugawara/analyzer/expression_analyzer.h>
#include <yugawara/binding/factory.h>
#include <yugawara/storage/basic_configurable_provider.h>

#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/ops/verifier.h>
#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/kvs/writable_stream.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/utils/copy_field_data.h>

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

namespace storage = yugawara::storage;
namespace binding = yugawara::binding;
namespace descriptor = takatori::descriptor;

using yugawara::variable::nullity;
using yugawara::variable::criteria;
using yugawara::storage::table;
using yugawara::storage::index;
using yugawara::storage::index_feature_set;

template <class Column>
std::vector<descriptor::variable> destinations(std::vector<Column>& columns) {
    std::vector<descriptor::variable> ret{};
    ret.reserve(columns.size());
    for(auto&& c : columns) {
        ret.emplace_back(c.destination());
    }
    return ret;
}

template <class Column>
std::vector<descriptor::variable> sources(std::vector<Column>& columns) {
    std::vector<descriptor::variable> ret{};
    ret.reserve(columns.size());
    for(auto&& c : columns) {
        ret.emplace_back(c.source());
    }
    return ret;
}

inline variable_table_info create_variable_table_info(
    std::vector<descriptor::variable> const& variables,
    jogasaki::mock::basic_record const& rec
) {
    std::unordered_map<descriptor::variable, value_info> map{};
    variable_table_info ret{};
    auto meta = rec.record_meta();
    std::size_t i = 0;
    map.reserve(variables.size());
    for(auto&& v : variables) {
        map.emplace(v, value_info{meta->value_offset(i), meta->nullity_offset(i), i});
        ++i;
    }
    return {std::move(map), std::move(meta)};
};

class operator_test_utils {
public:
    std::shared_ptr<table> create_table(table&& element) {
        return tables_->add_table(std::move(element));
    }

    std::vector<index::key> keys(
        std::shared_ptr<table> const& t,
        std::initializer_list<std::size_t> key_indices
    ) {
        std::vector<index::key> ret{};
        for (auto i : key_indices) {
            ret.emplace_back(t->columns()[i]);
        }
        return ret;
    }
    std::vector<index::column_ref> values(
        std::shared_ptr<table> const& t,
        std::initializer_list<std::size_t> value_indices
    ) {
        std::vector<index::column_ref> ret{};
        for (auto i : value_indices) {
            ret.emplace_back(t->columns()[i]);
        }
        return ret;
    }

    std::shared_ptr<index> create_primary_index(
        std::shared_ptr<table> const& t,
        std::initializer_list<std::size_t> key_indices,
        std::initializer_list<std::size_t> value_indices
    ) {
        auto k = keys(t, key_indices);
        auto v = values(t, value_indices);
        return tables_->add_index(std::make_shared<index>(
            t,
            index::simple_name_type(t->simple_name()),
            std::move(k),
            std::move(v),
            index_feature_set{
                ::yugawara::storage::index_feature::find,
                ::yugawara::storage::index_feature::scan,
                ::yugawara::storage::index_feature::unique,
                ::yugawara::storage::index_feature::primary,
            }
        ));
    }
    std::shared_ptr<index> create_secondary_index(
        std::shared_ptr<table> const& t,
        std::string_view name,
        std::initializer_list<std::size_t> key_indices,
        std::initializer_list<std::size_t> value_indices
    ) {
        auto k = keys(t, key_indices);
        auto v = values(t, value_indices);
        return tables_->add_index(std::make_shared<index>(
            t,
            index::simple_name_type(name),
            std::move(k),
            std::move(v),
            index_feature_set{
                ::yugawara::storage::index_feature::find,
                ::yugawara::storage::index_feature::scan,
            }
        ));
    }

    std::shared_ptr<storage::configurable_provider> tables_ = std::make_shared<storage::configurable_provider>();  //NOLINT
    std::shared_ptr<yugawara::variable::configurable_provider> variables_ = std::make_shared<yugawara::variable::configurable_provider>();  //NOLINT
    binding::factory bindings_{};  //NOLINT
    takatori::plan::graph_type plan_;  //NOLINT
    takatori::plan::process& process_;  //NOLINT

    memory::page_pool pool_{};  //NOLINT
    request_context request_context_{}; //NOLINT
    memory::lifo_paged_memory_resource resource_;  //NOLINT
    memory::lifo_paged_memory_resource varlen_resource_;  //NOLINT
    memory::lifo_paged_memory_resource verifier_varlen_resource_;  //NOLINT

    operator_test_utils() :
        process_(plan_.insert(takatori::plan::process {})),
        resource_(&pool_),
        varlen_resource_(&pool_),
        verifier_varlen_resource_(&pool_)
    {}

    std::shared_ptr<yugawara::analyzer::variable_mapping> variable_map_ =  //NOLINT
        std::make_shared<yugawara::analyzer::variable_mapping>();
    std::shared_ptr<yugawara::analyzer::expression_mapping> expression_map_ =  //NOLINT
        std::make_shared<yugawara::analyzer::expression_mapping>();

    std::shared_ptr<yugawara::compiled_info> compiler_info_{};  //NOLINT
    std::shared_ptr<processor_info> processor_info_;  //NOLINT

    relation::step::offer& add_offer(
        std::vector<descriptor::variable> stream_variables
    ) {
        std::vector<descriptor::variable> xch_columns;
        for(std::size_t i=0; i < stream_variables.size(); ++i) {
            xch_columns.emplace_back(
                bindings_.exchange_column()
            );
        }
        auto& f1 = plan_.insert(std::make_unique<takatori::plan::forward>(std::move(xch_columns)));
        // without offer, the columns are not used and block variables become empty
        using offer = relation::step::offer;
        std::vector<offer::column> offer_columns{};
        for(std::size_t i=0; i < stream_variables.size(); ++i) {
            offer_columns.emplace_back(
                stream_variables[i], f1.columns()[i]
            );
        }

        auto&& r1 = process_.operators().insert(relation::step::offer {
            bindings_.exchange(f1),
            std::move(offer_columns),
        });
        return r1;

    }

    relation::step::take_flat& add_take(
        std::size_t variable_count
    ) {
        std::vector<descriptor::variable> xch_columns;
        for(std::size_t i=0; i < variable_count; ++i) {
            xch_columns.emplace_back(
                bindings_.exchange_column()
            );
        }
        auto& f1 = plan_.insert(std::make_unique<takatori::plan::forward>(std::move(xch_columns)));
        using take = relation::step::take_flat;
        std::vector<take::column> take_columns{};
        for(std::size_t i=0; i < variable_count; ++i) {
            take_columns.emplace_back(
                f1.columns()[i], bindings_.stream_variable()
            );
        }

        auto&& r1 = process_.operators().insert(take{
            bindings_.exchange(f1),
            std::move(take_columns),
        });
        return r1;
    }

    /**
     * @brief Named bundle of upstream stream variables and their source record_meta.
     *
     * Returned as the second element of add_upstream_record_provider().
     * Supports index access (in[0], in[1], …) for individual descriptor::variable
     * values.  Carries the record_meta used when registering the variable types so
     * that set_variables() can interpret a raw record_ref without a basic_record wrapper.
     */
    struct input_definition {
        std::vector<descriptor::variable> vars_;  //NOLINT
        maybe_shared_ptr<meta::record_meta> meta_;  //NOLINT

        input_definition() = default;
        input_definition(
            std::vector<descriptor::variable> vars,
            maybe_shared_ptr<meta::record_meta> meta
        ) : vars_{std::move(vars)}, meta_{std::move(meta)} {}

        /// @brief Return the i-th upstream stream variable.
        descriptor::variable const& operator[](std::size_t i) const noexcept { return vars_[i]; }
    };

    /**
     * @brief Add an upstream record provider using a shared record_meta.
     *
     * Field types are extracted from the meta and bound to the take_flat stream
     * variables.  The meta is stored in the returned input_definition so that
     * set_variables() can interpret a record_ref of the same layout.
     *
     * @code
     *   auto [up, in] = add_upstream_record_provider(input.record_meta());
     * @endcode
     *
     * @param meta_ptr shared_ptr to the record_meta for the upstream record layout
     * @return pair of (take_flat reference-wrapper, input_definition carrying meta_ptr)
     */
    std::pair<std::reference_wrapper<relation::step::take_flat>, input_definition>
    add_upstream_record_provider(maybe_shared_ptr<meta::record_meta> const& meta_ptr) {
        auto const& meta = *meta_ptr;
        std::size_t const n = meta.field_count();
        auto& take_op = add_take(n);
        for (std::size_t i = 0; i < n; ++i) {
            auto res = make_variable_resolution(meta.at(i));
            variable_map_->bind(take_op.columns()[i].source(), res, true);
            variable_map_->bind(take_op.columns()[i].destination(), res, true);
        }
        std::vector<descriptor::variable> vars;
        vars.reserve(n);
        for (std::size_t i = 0; i < n; ++i) {
            vars.push_back(take_op.columns()[i].destination());
        }
        return {take_op, input_definition{std::move(vars), meta_ptr}};
    }

    /**
     * @brief Named bundle of upstream group stream variables returned by add_upstream_group_provider().
     *
     * Key and value variables are stored in separate vectors in registration order.
     * Use key(i) / value(i) to access individual variables, and key_input() / value_input()
     * to obtain input_definition objects suitable for set_variables().
     */
    struct group_input_definition {
        std::vector<descriptor::variable> key_vars_;     //NOLINT
        std::vector<descriptor::variable> value_vars_;   //NOLINT
        maybe_shared_ptr<meta::record_meta> key_meta_;   //NOLINT
        maybe_shared_ptr<meta::record_meta> value_meta_; //NOLINT

        /// @brief Return the i-th key stream variable.
        descriptor::variable const& key(std::size_t i) const noexcept { return key_vars_[i]; }
        /// @brief Return the i-th value stream variable.
        descriptor::variable const& value(std::size_t i) const noexcept { return value_vars_[i]; }

        /**
         * @brief Return an input_definition covering only key columns.
         *
         * Pass to set_variables() together with a basic_record whose fields match the key
         * type sequence used when constructing this provider.
         */
        input_definition key_input() const {
            return input_definition{key_vars_, key_meta_};
        }

        /**
         * @brief Return an input_definition covering only value columns.
         *
         * Pass to set_variables() together with a basic_record whose fields match the value
         * type sequence used when constructing this provider.
         */
        input_definition value_input() const {
            return input_definition{value_vars_, value_meta_};
        }
    };

    /**
     * @brief Add an upstream group provider for passive group operators (e.g. aggregate_group).
     *
     * Inserts a @c takatori::plan::group exchange and a @c relation::step::take_group node into the
     * current process graph.  All key and value stream variables are bound to their respective types
     * in @c variable_map_.
     *
     * @code
     *   auto key_meta = make_basic_record_meta({meta::int8_type()});
     *   auto val_meta = make_basic_record_meta({meta::int8_type(), meta::int8_type()});
     *   auto [up, in] = add_upstream_group_provider(key_meta, val_meta);
     *   // in.key(0), in.value(0), in.value(1), ...
     * @endcode
     *
     * @param key_meta_ptr   record_meta for the group-key columns
     * @param value_meta_ptr record_meta for the non-key (value) columns
     * @return pair of (take_group reference-wrapper, group_input_definition)
     */
    std::pair<std::reference_wrapper<relation::step::take_group>, group_input_definition>
    add_upstream_group_provider(
        maybe_shared_ptr<meta::record_meta> const& key_meta_ptr,
        maybe_shared_ptr<meta::record_meta> const& value_meta_ptr
    ) {
        auto const& key_meta = *key_meta_ptr;
        auto const& value_meta = *value_meta_ptr;
        std::size_t const n_keys = key_meta.field_count();
        std::size_t const n_values = value_meta.field_count();
        std::size_t const n = n_keys + n_values;

        // Create all exchange columns (keys first, then values).
        std::vector<descriptor::variable> xch_cols;
        xch_cols.reserve(n);
        for (std::size_t i = 0; i < n; ++i) {
            xch_cols.emplace_back(bindings_.exchange_column());
        }

        // Group-key columns are the first n_keys exchange columns.
        std::vector<descriptor::variable> key_xch_cols(
            xch_cols.begin(),
            xch_cols.begin() + static_cast<std::ptrdiff_t>(n_keys)
        );

        // Insert the group exchange into the plan.
        auto& grp = plan_.insert(
            std::make_unique<takatori::plan::group>(xch_cols, std::move(key_xch_cols))
        );

        // Build take_group column mappings: each exchange column → new stream variable.
        std::vector<relation::step::take_group::column> take_cols;
        take_cols.reserve(n);
        std::vector<descriptor::variable> key_stream_vars;
        std::vector<descriptor::variable> value_stream_vars;
        key_stream_vars.reserve(n_keys);
        value_stream_vars.reserve(n_values);
        for (std::size_t i = 0; i < n; ++i) {
            auto sv = bindings_.stream_variable();
            take_cols.emplace_back(grp.columns()[i], sv);
            if (i < n_keys) {
                key_stream_vars.push_back(sv);
            } else {
                value_stream_vars.push_back(sv);
            }
        }

        auto& take_op = process_.operators().insert(
            relation::step::take_group{bindings_.exchange(grp), std::move(take_cols)}
        );

        // Bind field types from key_meta to key columns.
        for (std::size_t i = 0; i < n_keys; ++i) {
            auto res = make_variable_resolution(key_meta.at(i));
            variable_map_->bind(take_op.columns()[i].source(), res, true);
            variable_map_->bind(take_op.columns()[i].destination(), res, true);
        }
        // Bind field types from value_meta to value columns.
        for (std::size_t i = 0; i < n_values; ++i) {
            auto res = make_variable_resolution(value_meta.at(i));
            variable_map_->bind(take_op.columns()[n_keys + i].source(), res, true);
            variable_map_->bind(take_op.columns()[n_keys + i].destination(), res, true);
        }

        return {take_op, group_input_definition{
            std::move(key_stream_vars),
            std::move(value_stream_vars),
            key_meta_ptr,
            value_meta_ptr
        }};
    }

    /**
     * @brief Create a vector of fresh stream variables to represent operator output columns.
     *
     * @param count number of output variables to create
     * @return vector of @p count new anonymous stream variables
     */
    std::vector<descriptor::variable> create_output_variables(std::size_t count) {
        std::vector<descriptor::variable> vars;
        vars.reserve(count);
        for (std::size_t i = 0; i < count; ++i) {
            vars.push_back(bindings_.stream_variable());
        }
        return vars;
    }

    /**
     * @brief Build a record_meta using basic_record's fixed per-field layout.
     *
     * All fields are nullable.  The layout matches that produced by
     * create_nullable_record<>(), so basic_record::operator== works between
     * records built with this meta and those built from matching Kind lists.
     *
     * @param types field types for the new meta
     * @return shared_ptr to the new record_meta
     */
    static std::shared_ptr<meta::record_meta> make_basic_record_meta(
        std::vector<meta::field_type> types) {
        std::size_t const n = types.size();
        std::vector<std::size_t> val_offs;
        std::vector<std::size_t> null_offs;
        val_offs.reserve(n);
        null_offs.reserve(n);
        boost::dynamic_bitset<std::uint64_t> nullability{n};
        nullability.set();
        for (std::size_t i = 0; i < n; ++i) {
            val_offs.push_back(i * basic_record_field_size);
            null_offs.push_back(n * basic_record_field_size * bits_per_byte + i);
        }
        return std::make_shared<meta::record_meta>(
            std::move(types),
            std::move(nullability),
            std::move(val_offs),
            std::move(null_offs),
            basic_record_field_alignment,
            (n + 1) * basic_record_field_size);
    }

    /**
     * @brief Copy values from a source record_ref into named variable slots.
     *
     * Field i of @p src (interpreted via @p in.meta_) is written to @p in.vars_[i]
     * in @p vt.  The caller must ensure the source record_ref was obtained from
     * the same record whose meta was passed to add_upstream_record_provider().
     *
     * @param vt  variable_table owning the destination block and layout info
     * @param in  input_definition carrying the variable list and source meta
     * @param src record_ref of the source record (same layout as in.meta_)
     */
    void set_variables(
        variable_table& vt,
        input_definition const& in,
        accessor::record_ref src) {
        accessor::record_ref dst = vt.store().ref();
        variable_table_info const& info = vt.info();
        auto const& src_meta = *in.meta_;
        for (std::size_t i = 0; i < in.vars_.size(); ++i) {
            auto const& vi = info.at(in.vars_[i]);
            utils::copy_nullable_field(
                src_meta.at(i),
                dst, vi.value_offset(), vi.nullity_offset(),
                src, src_meta.value_offset(i), src_meta.nullity_offset(i),
                &verifier_varlen_resource_);
        }
    }

    /**
     * @brief Build a basic_record from named variable slots in a variable block.
     *
     * The returned record uses basic_record's fixed per-field layout so it can
     * be compared against create_nullable_record<>() results with matching field
     * types via basic_record::operator==.
     *
     * @param vt   variable_table owning the source block and layout info
     * @param vars ordered list of variables to extract
     * @return a new basic_record containing the extracted values
     */
    jogasaki::mock::basic_record get_variables(
        variable_table& vt,
        std::vector<descriptor::variable> const& vars) {
        accessor::record_ref src = vt.store().ref();
        variable_table_info const& info = vt.info();
        auto const& block_meta = *info.meta();
        std::vector<meta::field_type> types;
        types.reserve(vars.size());
        for (auto const& var : vars) {
            types.push_back(block_meta.at(info.at(var).index()));
        }
        auto out_meta = make_basic_record_meta(types);
        jogasaki::mock::basic_record rec{out_meta};
        for (std::size_t i = 0; i < vars.size(); ++i) {
            auto const& vi = info.at(vars[i]);
            utils::copy_nullable_field(
                types[i],
                rec.ref(), out_meta->value_offset(i), out_meta->nullity_offset(i),
                src, vi.value_offset(), vi.nullity_offset(),
                &verifier_varlen_resource_);
        }
        return rec;
    }

    /**
     * @brief Bundle type for the downstream sink created by add_downstream_record_verifier.
     *
     * @details
     * - Connect the test-target operator's output port to input() before calling
     *   create_processor_info().
     * - Pass take() to the operator constructor as the downstream unique_ptr.
     * - After make_*_executor() has called take(), call set_body() to register the
     *   verification lambda.  The lambda may freely reference the executor by value
     *   since it is fully constructed at that point.
     */
    struct record_verifier_sink {
        std::unique_ptr<verifier> downstream_{};  //NOLINT
        std::reference_wrapper<relation::step::offer> offer_node_;  //NOLINT
        std::shared_ptr<std::function<void()>> body_ptr_{};  //NOLINT

        auto& input() noexcept { return offer_node_.get().input(); }
        std::unique_ptr<verifier> take() noexcept { return std::move(downstream_); }

        /**
         * @brief Assign the verification lambda after the executor has been constructed.
         *
         * The lambda is stored through a shared_ptr so the verifier (already moved
         * into the operator) will pick it up when it fires.
         *
         * @param f verification lambda, invoked once per processed record
         */
        void set_body(std::function<void()> f) { *body_ptr_ = std::move(f); }
    };

    /**
     * @brief Create a downstream record verifier sink.
     *
     * Internally inserts an offer node (so that the given stream variables are included
     * in the processor_info variable block) and creates a verifier that indirects
     * through a shared body pointer.  After the executor has been constructed with
     * make_*_executor(), register the verification logic with set_body().
     *
     * The offer is an implementation detail; callers connect to the returned sink via
     * input() and never interact with the offer directly.
     *
     * @param vars stream variables that must appear in the operator's variable block
     * @return record_verifier_sink – call input(), take(), and set_body() as needed
     */
    record_verifier_sink add_downstream_record_verifier(
        std::vector<descriptor::variable> vars) {
        auto& off = add_offer(std::move(vars));
        auto body_ptr = std::make_shared<std::function<void()>>();
        record_verifier_sink sink{
            std::make_unique<verifier>([body_ptr]() { (*body_ptr)(); }),
            off,
            body_ptr
        };
        return sink;
    }

    /**
     * @brief Emplace an operator node into the process graph.
     *
     * Wraps process_.operators().emplace<Op>() so that test code does not need to
     * access process_ directly.
     *
     * @tparam Op  operator node type to emplace (e.g. relation::project)
     * @tparam Args constructor argument types, deduced
     * @param args  constructor arguments forwarded to Op
     * @return reference to the newly emplaced operator node
     */
    template <class Op, class ...Args>
    Op& emplace_operator(Args&&... args) {
        return process_.operators().emplace<Op>(std::forward<Args>(args)...);
    }

    void create_processor_info(variable_table* host_variables = nullptr, bool validate = false) {
        yugawara::analyzer::expression_analyzer resolver{expression_map_, variable_map_};
        EXPECT_TRUE(resolver.resolve(process_.operators(), validate));
        compiler_info_ = std::make_shared<yugawara::compiled_info>(expression_map_, variable_map_);
        processor_info_ = std::make_shared<processor_info>(process_.operators(), *compiler_info_, host_variables);
    }

    template <bool ForKey, class T, class ...Args>
    void add_types(T& target, Args&&... types) {
        std::vector<std::reference_wrapper<takatori::type::data>> v{types...};
        std::size_t i=0;
        for(auto&& type : v) {
            yugawara::analyzer::variable_resolution r{std::move(static_cast<takatori::type::data&>(type))};
            if constexpr (ForKey) {
                variable_map_->bind(target.keys()[i].source(), r, true);
                variable_map_->bind(target.keys()[i].destination(), r, true);
            } else {
                variable_map_->bind(target.columns()[i].source(), r, true);
                variable_map_->bind(target.columns()[i].destination(), r, true);
            }
            ++i;
        }
    }
    template <class T, class ...Args>
    void add_key_types(T& target, Args&&... types) {
        add_types<true>(target, types...);
    }
    template <class T, class ...Args>
    void add_column_types(T& target, Args&&... types) {
        add_types<false>(target, types...);
    }

    /**
     * @brief Create a variable_resolution from a runtime field_type.
     *
     * Maps the stored meta::field_type_kind (with its option) to the
     * corresponding takatori type, suitable for binding into variable_mapping.
     *
     * @param ft field type to convert
     * @return variable_resolution holding the equivalent takatori type
     */
    static yugawara::analyzer::variable_resolution make_variable_resolution(
        meta::field_type const& ft) {
        namespace tt = takatori::type;
        using k = meta::field_type_kind;
        switch (ft.kind()) {
            case k::boolean:   return tt::boolean{};
            case k::int4:      return tt::int4{};
            case k::int8:      return tt::int8{};
            case k::float4:    return tt::float4{};
            case k::float8:    return tt::float8{};
            case k::decimal: {
                auto const& opt = *ft.option<k::decimal>();
                return tt::decimal{opt.precision_, opt.scale_};
            }
            case k::character: {
                auto const& opt = *ft.option<k::character>();
                return tt::character{tt::varying_t{opt.varying_}, opt.length_};
            }
            case k::date:      return tt::date{};
            case k::time_of_day: {
                auto const& opt = *ft.option<k::time_of_day>();
                return tt::time_of_day{tt::with_time_zone_t{opt.with_offset_}};
            }
            case k::time_point: {
                auto const& opt = *ft.option<k::time_point>();
                return tt::time_point{tt::with_time_zone_t{opt.with_offset_}};
            }
            default: fail();
        }
        return {};
    }

    std::shared_ptr<yugawara::variable::declaration const> register_variable(std::string_view name, field_type_kind kind) {
        if (auto e = variables_->find(name)) {
            // ignore if it's already exists
            return e;
        }
        switch(kind) {
            case field_type_kind::int4: return variables_->add({name, takatori::type::int4{}}, false);
            case field_type_kind::int8: return variables_->add({name, takatori::type::int8{}}, false);
            case field_type_kind::float4: return variables_->add({name, takatori::type::float4{}}, false);
            case field_type_kind::float8: return variables_->add({name, takatori::type::float8{}}, false);
            case field_type_kind::character: return variables_->add({name, takatori::type::character{takatori::type::varying}}, false);
            case field_type_kind::decimal: return variables_->add({name, takatori::type::decimal{}}, false);
            case field_type_kind::date: return variables_->add({name, takatori::type::date{}}, false);
            case field_type_kind::time_of_day: return variables_->add({name, takatori::type::time_of_day{}}, false);
            case field_type_kind::time_point: return variables_->add({name, takatori::type::time_point{}}, false);
            default: fail();
        }
        return {};
    }

};

} // namespace jogasaki::executor::process::impl::ops
