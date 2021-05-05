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

#include <string>

#include <gtest/gtest.h>

#include <takatori/plan/forward.h>
#include <takatori/relation/step/offer.h>
#include <takatori/type/character.h>
#include <takatori/value/character.h>
#include <takatori/type/int.h>
#include <takatori/value/int.h>
#include <takatori/util/object_creator.h>
#include <yugawara/binding/factory.h>
#include <yugawara/storage/basic_configurable_provider.h>

#include <jogasaki/executor/process/impl/variable_table.h>
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

using yugawara::variable::nullity;
using yugawara::variable::criteria;
using yugawara::storage::table;
using yugawara::storage::index;
using yugawara::storage::index_feature_set;

class operator_test_utils {
public:
    std::shared_ptr<table> create_table(table&& element) {
        return tables_->add_table(std::move(element));
    }

    std::vector<index::key, takatori::util::object_allocator<index::key>> keys(
        std::shared_ptr<table> const& t,
        std::initializer_list<std::size_t> key_indices
    ) {
        std::vector<index::key, takatori::util::object_allocator<index::key>> ret{};
        for (auto i : key_indices) {
            ret.emplace_back(t->columns()[i]);
        }
        return ret;
    }
    std::vector<index::column_ref, takatori::util::object_allocator<index::column_ref>> values(
        std::shared_ptr<table> const& t,
        std::initializer_list<std::size_t> value_indices
    ) {
        std::vector<index::column_ref, takatori::util::object_allocator<index::column_ref>> ret{};
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
    binding::factory bindings_{};  //NOLINT
    takatori::plan::graph_type plan_;  //NOLINT
    takatori::plan::process& process_;  //NOLINT

    memory::page_pool pool_{};  //NOLINT
    memory::lifo_paged_memory_resource resource_;  //NOLINT
    memory::lifo_paged_memory_resource varlen_resource_;  //NOLINT

    operator_test_utils() :
        process_(plan_.insert(takatori::plan::process {})),
        resource_(&pool_),
        varlen_resource_(&pool_)
    {}
    takatori::util::object_creator creator_{};  //NOLINT
    relation::find* target_{};  //NOLINT

    std::shared_ptr<yugawara::analyzer::variable_mapping> variable_map_ =  //NOLINT
        std::make_shared<yugawara::analyzer::variable_mapping>();
    std::shared_ptr<yugawara::analyzer::expression_mapping> expression_map_ =  //NOLINT
        std::make_shared<yugawara::analyzer::expression_mapping>();

    std::shared_ptr<yugawara::compiled_info> compiler_info_{};  //NOLINT
    std::shared_ptr<processor_info> processor_info_;  //NOLINT

    void add_downstream(
        std::vector<variable> stream_variables
    ) {
        std::vector<descriptor::variable, takatori::util::object_allocator<descriptor::variable>> xch_columns;
        for(std::size_t i=0; i < stream_variables.size(); ++i) {
            xch_columns.emplace_back(
                bindings_.exchange_column()
            );
        }
        auto& f1 = plan_.insert(creator_.create_unique<takatori::plan::forward>(std::move(xch_columns)));
        // without offer, the columns are not used and block variables become empty
        using offer = relation::step::offer;
        std::vector<offer::column, takatori::util::object_allocator<offer::column>> offer_columns{};
        for(std::size_t i=0; i < stream_variables.size(); ++i) {
            offer_columns.emplace_back(
                stream_variables[i], f1.columns()[i]
            );
        }

        auto&& r1 = process_.operators().insert(relation::step::offer {
            bindings_.exchange(f1),
            std::move(offer_columns),
        });

        target_->output() >> r1.input(); // connection required by takatori
    }

    void create_processor_info() {
        compiler_info_ = std::make_shared<yugawara::compiled_info>(expression_map_, variable_map_);
        processor_info_ = std::make_shared<processor_info>(process_.operators(), *compiler_info_);
    }
};

}

