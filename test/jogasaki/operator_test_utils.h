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

#include <string>

#include <gtest/gtest.h>

#include <takatori/plan/forward.h>
#include <takatori/relation/step/offer.h>
#include <takatori/relation/step/take_flat.h>
#include <takatori/type/character.h>
#include <takatori/value/character.h>
#include <takatori/type/int.h>
#include <takatori/type/decimal.h>
#include <takatori/type/date.h>
#include <takatori/type/time_of_day.h>
#include <takatori/type/time_point.h>
#include <takatori/value/int.h>
#include <yugawara/binding/factory.h>
#include <yugawara/storage/basic_configurable_provider.h>

#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/kvs/writable_stream.h>

#include <jogasaki/mock/basic_record.h>

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

    void create_processor_info(variable_table* host_variables = nullptr) {
        compiler_info_ = std::make_shared<yugawara::compiled_info>(expression_map_, variable_map_);
        processor_info_ = std::make_shared<processor_info>(process_.operators(), *compiler_info_, host_variables);
    }

    template <class ...Args>
    void add_types(std::vector<descriptor::variable>& vars, Args&&... types) {
        std::vector<std::reference_wrapper<takatori::type::data>> v{types...};
        std::size_t i=0;
        for(auto&& type : v) {
            yugawara::analyzer::variable_resolution r{std::move(static_cast<takatori::type::data&>(type))};
            variable_map_->bind(vars[i], r, true);
            ++i;
        }
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
