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
#include "batch_block_executor.h"

#include <atomic>
#include <sstream>
#include <string_view>
#include <type_traits>
#include <vector>
#include <glog/logging.h>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/accessor/text.h>
#include <jogasaki/api/impl/parameter_set.h>
#include <jogasaki/api/impl/prepared_statement.h>
#include <jogasaki/api/parameter_set.h>
#include <jogasaki/api/statement_handle.h>
#include <jogasaki/data/any.h>
#include <jogasaki/error/error_info.h>
#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/batch/batch_execution_info.h>
#include <jogasaki/executor/batch/batch_execution_state.h>
#include <jogasaki/executor/executor.h>
#include <jogasaki/executor/file/file_reader.h>
#include <jogasaki/executor/file/loader.h>
#include <jogasaki/executor/file/parquet_reader.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/kvs/transaction_option.h>
#include <jogasaki/logging.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_traits.h>
#include <jogasaki/plan/mirror_container.h>
#include <jogasaki/plan/parameter_set.h>
#include <jogasaki/plan/prepared_statement.h>
#include <jogasaki/request_statistics.h>
#include <jogasaki/status.h>
#include <jogasaki/utils/fail.h>

#include "batch_executor.h"
#include "batch_file_executor.h"

namespace jogasaki::executor::batch {

meta::field_type_kind host_variable_type(executor::process::impl::variable_table_info const& vinfo, std::string_view name) {
    auto idx = vinfo.at(name).index();
    return vinfo.meta()->at(idx).kind();
}

void set_parameter(api::parameter_set& ps, accessor::record_ref ref, std::unordered_map<std::string, file::parameter> const& mapping) {
    auto pset = static_cast<api::impl::parameter_set&>(ps).body();  //NOLINT
    for(auto&& [name, param] : mapping) {
        if(ref.is_null(param.nullity_offset_)) {
            pset->set_null(name);
            continue;
        }
        using kind = meta::field_type_kind;
        switch(param.type_) {
            case meta::field_type_kind::int4: pset->set_int4(name, ref.get_value<runtime_t<kind::int4>>(param.value_offset_)); break;
            case meta::field_type_kind::int8: pset->set_int8(name, ref.get_value<runtime_t<kind::int8>>(param.value_offset_)); break;
            case meta::field_type_kind::float4: pset->set_float4(name, ref.get_value<runtime_t<kind::float4>>(param.value_offset_)); break;
            case meta::field_type_kind::float8: pset->set_float8(name, ref.get_value<runtime_t<kind::float8>>(param.value_offset_)); break;
            case meta::field_type_kind::character: pset->set_character(name, ref.get_value<runtime_t<kind::character>>(param.value_offset_)); break;
            case meta::field_type_kind::octet: pset->set_octet(name, ref.get_value<runtime_t<kind::octet>>(param.value_offset_)); break;
            case meta::field_type_kind::decimal: pset->set_decimal(name, ref.get_value<runtime_t<kind::decimal>>(param.value_offset_)); break;
            case meta::field_type_kind::date: pset->set_date(name, ref.get_value<runtime_t<kind::date>>(param.value_offset_)); break;
            case meta::field_type_kind::time_of_day: pset->set_time_of_day(name, ref.get_value<runtime_t<kind::time_of_day>>(param.value_offset_)); break;
            case meta::field_type_kind::time_point: pset->set_time_point(name, ref.get_value<runtime_t<kind::time_point>>(param.value_offset_)); break;
            default: fail_with_exception();
        }
    }
}

file::reader_field_locator create_locator(std::string_view name, std::shared_ptr<plan::parameter_set> const& pset) {
    for(auto&& [n, e] : *pset) {
        if(name != n) continue;
        if(e.type().kind() == meta::field_type_kind::reference_column_position) {
            auto idx = e.as_any().to<std::size_t>();
            return {"", idx};
        }
        if(e.type().kind() == meta::field_type_kind::reference_column_name) {
            auto t = e.as_any().to<accessor::text>();
            auto referenced = static_cast<std::string_view>(t);
            return {referenced, file::npos};
        }
    }
    return {};
}

void create_reader_option_and_maping(
    api::parameter_set const& ps,
    api::statement_handle prepared,
    std::unordered_map<std::string, file::parameter>& mapping,
    file::reader_option& out
) {
    auto pset = static_cast<api::impl::parameter_set const&>(ps).body();  //NOLINT
    auto stmt = reinterpret_cast<api::impl::prepared_statement*>(prepared.get()); //NOLINT
    auto vinfo = stmt->body()->mirrors()->host_variable_info();
    std::vector<file::reader_field_locator> locs{};

    // create mapping
    mapping.clear();
    mapping.reserve(vinfo->meta()->field_count());
    locs.resize(vinfo->meta()->field_count());
    for(auto it = vinfo->name_list_begin(), end = vinfo->name_list_end(); it != end; ++it) {
        auto& name = it->first;
        auto& v = vinfo->at(name);
        auto loc = create_locator(name, pset);
        locs[v.index()] = loc;
        if(loc.empty_) continue;
        mapping[name] = file::parameter{
            host_variable_type(*vinfo, name),
            v.index(),
            vinfo->meta()->value_offset(v.index()),
            vinfo->meta()->nullity_offset(v.index())
        };
    }
    out = {std::move(locs), *vinfo->meta()};
}

std::pair<bool, bool> batch_block_executor::next_statement() {
    if (state_->error_aborting()) {
        return {false, false};
    }

    // read records, assign host variables, submit tasks
    auto ps = std::shared_ptr<api::parameter_set>{info_.parameters()->clone()};
    if(! reader_) {
        file::reader_option opt{};
        create_reader_option_and_maping(*info_.parameters(), info_.prepared(), mapping_, opt);
        reader_ = file::parquet_reader::open(file_, std::addressof(opt), block_index_);
        if(! reader_) {
            state_->set_error_status(
                status::err_io_error,
                create_error_info(error_code::load_file_exception, "opening parquet file failed.", status::err_io_error)
            );
            finish(info_, *state_);
            return {false, false};
        }

        if(auto res = executor::create_transaction(
               *info_.db(),
               tx_,
               std::make_shared<kvs::transaction_option>(
                   kvs::transaction_option::transaction_type::occ,
                   std::vector<std::string>{},
                   std::vector<std::string>{},
                   std::vector<std::string>{}
               )
           );
           res != status::ok) {
            state_->set_error_status(
                res,
                create_error_info(error_code::sql_execution_exception, "starting new tx failed.", res)
            );
            reader_->close();
            reader_.reset();
            finish(info_, *state_);
            // currently handled as unrecoverable error
            // TODO limit the number of tx used by batch executor
            return {false, false};
        }
    }

    accessor::record_ref ref{};
    if(! reader_->next(ref)) {
        reader_->close();
        reader_.reset();

        if(state_->error_aborting()) {
            return {false, false};
        }
        if(auto res = tx_->commit(); res != status::ok) {
            if(res == status::err_serialization_failure) {
                state_->set_error_status(
                    res,
                    create_error_info(error_code::cc_exception, "Committing tx failed.", res)
                );
            } else {
                state_->set_error_status(
                    res,
                    create_error_info(error_code::sql_service_exception, "Unexpected error occurred on commit.", res)
                );
            }
            finish(info_, *state_);
            return {false, false};
        }
        return {true, false};
    }

    set_parameter(*ps, ref, mapping_);

    if (state_->error_aborting()) {
        return {false, false};
    }
    std::shared_ptr<batch_executor> r = root() ? root()->shared() : nullptr;
    ++state_->running_statements();
    executor::execute_async(
        *info_.db(),
        tx_,
        info_.prepared(),
        std::move(ps),
        nullptr,
        [&, state = state_, root = std::move(r)](
            status st,
            std::shared_ptr<error::error_info> err_info,  //NOLINT(performance-unnecessary-value-param)
            std::shared_ptr<request_statistics> stats  //NOLINT(performance-unnecessary-value-param)
        ) {
            (void) root; // let callback own the tree root
            (void) stats;  // TODO implement stats for load
            --state->running_statements();
            if(state->error_aborting()) {
                return;
            }
            auto pos = statements_executed_++;
            if(st != status::ok) {
                // add loaded file information as additional text
                std::stringstream ss{};
                ss << "file:" << file_ <<
                    " block index:" << block_index_ <<
                    " statement position:" << pos <<
                    " status:" << st;
                err_info->additional_text(ss.str());
                state_->set_error_status(st, std::move(err_info));
                finish(info_, *state_);
                constexpr auto lp = "/:jogasaki:executor:batch:batch_block_executor:next_statement ";
                VLOG(log_error) << lp << ss.str();  //NOLINT
                return;
            }
            end_of_statement();
        }
    );
    return {true, true};
}

batch_executor *batch_block_executor::root() const noexcept {
    if(! parent_) return nullptr;
    return parent_->parent();
}

batch_file_executor *batch_block_executor::parent() const noexcept {
    return parent_;
}

std::size_t batch_block_executor::statements_executed() const noexcept {
    return statements_executed_;
}

batch_block_executor::batch_block_executor(
    std::string file,
    std::size_t block_index,
    batch_execution_info info,
    std::shared_ptr<batch_execution_state> state,
    batch_file_executor* parent
) noexcept:
    file_(std::move(file)),
    block_index_(block_index),
    info_(std::move(info)),
    state_(std::move(state)),
    parent_(parent)
{}

std::shared_ptr<batch_block_executor>
batch_block_executor::create_block_executor(
    std::string file,
    std::size_t block_index,
    batch_execution_info info,
    std::shared_ptr<batch_execution_state> state,
    batch_file_executor *parent
) {
    return std::shared_ptr<batch_block_executor>(
        new batch_block_executor{
            std::move(file),
            block_index,
            std::move(info),
            std::move(state),
            parent
        }
    );
}

std::shared_ptr<batch_execution_state> const &batch_block_executor::state() const noexcept {
    return state_;
}

void batch_block_executor::end_of_statement() {
    // execute next statement
    auto [s, f] = next_statement();
    if(! s) {
        return;
    }
    if(! f) {
        if(! parent_) return; // for testing
        parent_->end_of_block(this);
    }
}

}

