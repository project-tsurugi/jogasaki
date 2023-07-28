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
#include "batch_block_executor.h"

#include <atomic>
#include <glog/logging.h>

#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/fail.h>

#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/transaction.h>
#include <jogasaki/api/impl/parameter_set.h>
#include <jogasaki/api/impl/prepared_statement.h>

#include "batch_executor.h"
#include "batch_file_executor.h"

namespace jogasaki::executor::batch {

using takatori::util::fail;

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
            default: fail();
        }
    }
}

file::parquet_reader_field_locator create_locator(std::string_view name, std::shared_ptr<plan::parameter_set> const& pset) {
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
    file::parquet_reader_option& out
) {
    auto pset = static_cast<api::impl::parameter_set const&>(ps).body();  //NOLINT
    auto stmt = reinterpret_cast<api::impl::prepared_statement*>(prepared.get()); //NOLINT
    auto vinfo = stmt->body()->mirrors()->host_variable_info();
    std::vector<file::parquet_reader_field_locator> locs{};

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

bool execute_statement_on_next_block(batch_file_executor& f) {
    auto&& [success, next_block] = f.next_block();
    if(! success) {
        // let's abort the batch - error_info already called
        return true;
    }
    if(next_block) {
        return next_block->execute_statement();
    }
    return false;
}

void batch_block_executor::find_and_process_next_block() {
    if(! parent_) return; // for testing
    auto to_exit = execute_statement_on_next_block(*parent_);

    auto self = parent_->release(this); // keep self by the end of this scope
    (void) self;

    if(to_exit) {
        return;
    }

    auto* r = root();
    if(! r) return; // for testing

    if(parent_->child_count() != 0) {
        return;
    }
    // no new block in the parent file
    auto parent = r->release(parent_);
    (void) parent;

    while(true) { // to repeat next file
        auto&& [success, next_file] = r->next_file();
        if(! success) {
            // let's abort the batch - error_info already called
            return;
        }
        if(! next_file) {
            // no more file
            break;
        }
        if(execute_statement_on_next_block(*next_file)) {
            return;
        }
        // file has no block - skip to next file
        r->release(next_file.get());
    }

    // no more file
    if(r->child_count() == 0) {
        // end of loader
        r->finish();
    }
}

bool batch_block_executor::execute_statement() {
    if (root() && root()->error_aborting()) {
        if(root()->running_statements() == 0) {
            // end of loader
            root()->finish();
        }
        return false;
    }

    // read records, assign host variables, submit tasks
    auto ps = std::shared_ptr<api::parameter_set>{parameters_->clone()};
    if(! reader_) {
        file::parquet_reader_option opt{};
        create_reader_option_and_maping(*parameters_, prepared_, mapping_, opt);
        reader_ = file::parquet_reader::open(file_, std::addressof(opt), block_index_);
        if(! reader_) {
            (void) root()->error_info(status::err_io_error, "opening parquet file failed.");
            return false;
        }

        if(auto res = api::impl::transaction::create_transaction(*db_, tx_,
                {kvs::transaction_option::transaction_type::occ, {}, {}, {}}); res != status::ok) {
            (void) root()->error_info(res, "starting new tx failed.");
            reader_->close();
            reader_.reset();
            // currently handled as unrecoverable error
            // TODO limit the number of tx used by batch executor
            return false;
        }
    }

    accessor::record_ref ref{};
    if(! reader_->next(ref)) {
        reader_->close();
        reader_.reset();

        if(auto res = tx_->commit_internal(); res != status::ok) {
            (void) root()->error_info(res, "committing tx failed.");
            return false;
        }
        find_and_process_next_block();
        return false;
    }

    set_parameter(*ps, ref, mapping_);

    if(root()) {
        ++root()->running_statements();
    }
    tx_->execute_async(prepared_,
        std::move(ps),
        nullptr,
        [&](status st, std::string_view msg){
            if(root()) {
                --root()->running_statements();
                if(root()->error_aborting()) {
                    // When error occurs during batch execution, callback and exit execution.
                    // Releasing executors are not done in small pieces,
                    // but it's left to the destruction of batch_executor to release all in bulk.
                    root()->finish();
                    return;
                }
            }
            ++statements_executed_;
            if(st != status::ok) {
                std::stringstream ss{};
                ss << "Executing statement failed. file:" << file_ <<
                    " block index:" << block_index_ <<
                    " statement position:" << statements_executed_ <<
                    " status:" << st <<
                    " message:\"" << msg << "\"";
                if(root()) {
                    (void) root()->error_info(st, ss.str());
                }
                VLOG_LP(log_error) << ss.str();  //NOLINT
                return;
            }

            // execute next statement
            execute_statement();
        }
    );
    return true;
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
    api::statement_handle prepared,
    maybe_shared_ptr<const api::parameter_set> parameters,
    api::impl::database *db,
    batch_file_executor* parent
) noexcept:
    file_(std::move(file)),
    block_index_(block_index),
    prepared_(prepared),
    parameters_(std::move(parameters)),
    db_(db),
    parent_(parent)
{}

std::shared_ptr<batch_block_executor>
batch_block_executor::create_block_executor(
    std::string file,
    std::size_t block_index,
    api::statement_handle prepared,
    maybe_shared_ptr<const api::parameter_set> parameters,
    api::impl::database *db,
    batch_file_executor *parent
) {
    auto ret = std::make_shared<batch_block_executor>(
        std::move(file),
        block_index,
        prepared,
        std::move(parameters),
        db,
        parent
    );
    // do init when needed
    return ret;
}

}

