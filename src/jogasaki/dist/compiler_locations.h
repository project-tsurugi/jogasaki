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
#include <map>
#include <memory>
#include <utility>
#include <yugawara/compiler_options.h>
#include <yugawara/remote/symbol_location_query.h>
#include <yugawara/remote/operation_location_query.h>
#include <jogasaki/dist/symbol_locations.h>
#include <jogasaki/dist/operation_location.h>

namespace jogasaki::dist {
/** @brief Exposes declarations from the retained catalog snapshot to the compiler. */
class compiler_symbol_locations final : public yugawara::remote::symbol_location_query {
public:
    explicit compiler_symbol_locations(std::shared_ptr<symbol_locations const> symbols) : symbols_(std::move(symbols)) {
        for (auto id : symbols_->available_location_ids()) {
            infos_.emplace(id, yugawara::remote::location_info{id, symbols_->of(id).name});
        }
    }
    yugawara::remote::location_info const& of(location_id_type id) const override { return infos_.at(id); }
    location_id_set_type const& available_location_ids() const override { return symbols_->available_location_ids(); }
    location_id_set_type find(yugawara::storage::table const& v) const override { return symbols_->find(v); }
    location_id_set_type find(yugawara::function::declaration const& v) const override { return symbols_->find(v); }
    location_id_set_type find(yugawara::aggregate::declaration const& v) const override { return symbols_->find(v); }
    location_id_set_type find(yugawara::variable::declaration const& v) const override { return symbols_->find(v); }
private:
    std::shared_ptr<symbol_locations const> symbols_;
    std::map<location_id_type, yugawara::remote::location_info> infos_{};
};

/** @brief Reads compiler-owned step placements before releasing the compiled statement. */
class compiler_operation_locations final : public operation_locations {
public:
    compiler_operation_locations(yugawara::remote::operation_location_query const& query,
                                 symbol_locations const& symbols) : query_(query), symbols_(symbols) {}
    location_id_set_type find(takatori::plan::step const& step) const override { return query_.find(step); }
    std::string location_name(location_id_type id) const override { return symbols_.of(id).name; }
private:
    yugawara::remote::operation_location_query const& query_;
    symbol_locations const& symbols_;
};

/** @brief Enables only the Phase-1 operations supported by Portable IR (L3-04). */
inline void configure_remote_compilation(yugawara::compiler_options& options,
                                         std::shared_ptr<symbol_locations const> const& symbols) {
    using relation = takatori::relation::expression_kind;
    using scalar = takatori::scalar::expression_kind;
    auto& locations = options.location_options();
    locations.symbol_location_query() = std::make_shared<compiler_symbol_locations>(symbols);
    locations.remote_relation_expression_kinds() = {
        relation::scan, relation::find, relation::filter, relation::project, relation::offer};
    // Casts stay on the coordinator: the compiler's kind-only allow list cannot express
    // the Portable IR type restrictions (in particular TINYINT and SMALLINT).
    locations.remote_scalar_expression_kinds() = {
        scalar::immediate, scalar::variable_reference, scalar::unary, scalar::binary, scalar::compare};
    locations.remote_step_kinds() = {takatori::plan::step_kind::process};
    locations.remote_statement_kinds() = {takatori::statement::statement_kind::execute};
    locations.allow_empty_operation_location() = false;
}
} // namespace jogasaki::dist
