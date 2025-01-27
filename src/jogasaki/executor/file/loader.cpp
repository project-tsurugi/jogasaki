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
#include "loader.h"

#include <atomic>
#include <cstddef>
#include <ostream>
#include <type_traits>
#include <glog/logging.h>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/accessor/text.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/impl/parameter_set.h>
#include <jogasaki/api/impl/prepared_statement.h>
#include <jogasaki/api/parameter_set.h>
#include <jogasaki/api/statement_handle.h>
#include <jogasaki/data/any.h>
#include <jogasaki/error/error_info.h>
#include <jogasaki/executor/executor.h>
#include <jogasaki/executor/file/file_reader.h>
#include <jogasaki/executor/file/parquet_reader.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_traits.h>
#include <jogasaki/plan/mirror_container.h>
#include <jogasaki/plan/parameter_set.h>
#include <jogasaki/plan/prepared_statement.h>
#include <jogasaki/request_statistics.h>
#include <jogasaki/status.h>
#include <jogasaki/transaction_context.h>
#include <jogasaki/utils/fail.h>

namespace jogasaki::executor::file {

using takatori::util::maybe_shared_ptr;

loader::loader(
    std::vector<std::string> files,
    api::statement_handle prepared,
    maybe_shared_ptr<api::parameter_set const> parameters,
    std::shared_ptr<transaction_context> tx,
    api::impl::database& db,
    std::size_t bulk_size
) noexcept:
    files_(std::move(files)),
    prepared_(prepared),
    parameters_(std::move(parameters)),
    tx_(std::move(tx)),
    db_(std::addressof(db)),
    next_file_(files_.begin()),
    bulk_size_(bulk_size)
{}

meta::field_type_kind
host_variable_type(executor::process::impl::variable_table_info const& vinfo, std::string_view name) {
    auto idx = vinfo.at(name).index();
    return vinfo.meta()->at(idx).kind();
}

void set_parameter(
    api::parameter_set& ps,
    accessor::record_ref ref,
    std::unordered_map<std::string, parameter> const& mapping
) {
    auto pset = static_cast<api::impl::parameter_set&>(ps).body();  //NOLINT
    for(auto&& [name, param] : mapping) {
        if(ref.is_null(param.nullity_offset_)) {
            pset->set_null(name);
            continue;
        }
        using kind = meta::field_type_kind;
        switch(param.type_) {
            case meta::field_type_kind::boolean: pset->set_boolean(name, ref.get_value<runtime_t<kind::boolean>>(param.value_offset_)); break;
            case meta::field_type_kind::int4: pset->set_int4(name, ref.get_value<runtime_t<kind::int4>>(param.value_offset_)); break;
            case meta::field_type_kind::int8: pset->set_int8(name, ref.get_value<runtime_t<kind::int8>>(param.value_offset_)); break;
            case meta::field_type_kind::float4: pset->set_float4(name, ref.get_value<runtime_t<kind::float4>>(param.value_offset_)); break;
            case meta::field_type_kind::float8: pset->set_float8(name, ref.get_value<runtime_t<kind::float8>>(param.value_offset_)); break;
            case meta::field_type_kind::decimal: pset->set_decimal(name, ref.get_value<runtime_t<kind::decimal>>(param.value_offset_)); break;
            case meta::field_type_kind::character: pset->set_character(name, ref.get_value<runtime_t<kind::character>>(param.value_offset_)); break;
            case meta::field_type_kind::octet: pset->set_octet(name, ref.get_value<runtime_t<kind::octet>>(param.value_offset_)); break;
            case meta::field_type_kind::date: pset->set_date(name, ref.get_value<runtime_t<kind::date>>(param.value_offset_)); break;
            case meta::field_type_kind::time_of_day: pset->set_time_of_day(name, ref.get_value<runtime_t<kind::time_of_day>>(param.value_offset_)); break;
            case meta::field_type_kind::time_point: pset->set_time_point(name, ref.get_value<runtime_t<kind::time_point>>(param.value_offset_)); break;
            default: fail_with_exception();
        }
    }
}

reader_field_locator create_locator(std::string_view name, std::shared_ptr<plan::parameter_set> const& pset) {
    for(auto&& [n, e] : *pset) {
        if(name != n) continue;
        if(e.type().kind() == meta::field_type_kind::reference_column_position) {
            auto idx = e.value().to<std::size_t>();
            return {"", idx};
        }
        if(e.type().kind() == meta::field_type_kind::reference_column_name) {
            auto t = e.value().to<std::string>();
            auto referenced = static_cast<std::string_view>(t);
            return {referenced, npos};
        }
    }
    return {};
}

void create_reader_option_and_maping(
    api::parameter_set const& ps,
    api::statement_handle prepared,
    std::unordered_map<std::string, parameter>& mapping,
    reader_option& out
) {
    auto pset = static_cast<api::impl::parameter_set const&>(ps).body();  //NOLINT
    auto stmt = reinterpret_cast<api::impl::prepared_statement*>(prepared.get()); //NOLINT
    auto vinfo = stmt->body()->mirrors()->host_variable_info();
    std::vector<reader_field_locator> locs{};

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
        mapping[name] = parameter{
            host_variable_type(*vinfo, name),
            v.index(),
            vinfo->meta()->value_offset(v.index()),
            vinfo->meta()->nullity_offset(v.index())
        };
    }
    out = {std::move(locs), *vinfo->meta()};
}

loader_result loader::operator()() {  //NOLINT(readability-function-cognitive-complexity)
    if (error_aborted_) {
        return loader_result::error;
    }
    if (error_aborting_) {
        if (running_statement_count_ == 0) {
            VLOG_LP(log_error) << "transaction is aborted due to the error during loading";
            // currently err_aborted should be used in order to report tx aborted. When abort can be reported
            // in different channel, original status code should be passed. TODO
            status_ = status::err_aborted;
            executor::abort_transaction(tx_, {}); // TODO fill request info
            VLOG_LP(log_info) << "transaction aborted";
            error_aborted_ = true;
            return loader_result::error;
        }
        return loader_result::running;
    }
    if (! more_to_read_) {
        return running_statement_count_ != 0 ? loader_result::running : loader_result::ok;
    }
    auto slots = bulk_size_ - running_statement_count_;
    if (slots == 0) {
        return loader_result::running;
    }
    for(std::size_t i=0; i < slots; ++i) {
        // read records, assign host variables, submit tasks
        auto ps = std::shared_ptr<api::parameter_set>{parameters_->clone()};
        if(! reader_) {
            if(next_file_ == files_.end()) {
                // reading all files completed
                more_to_read_ = false;
                return running_statement_count_ != 0 ? loader_result::running : loader_result::ok;
            }

            reader_option opt{};
            create_reader_option_and_maping(*parameters_, prepared_, mapping_, opt);
            reader_ = parquet_reader::open(*next_file_, std::addressof(opt));
            ++next_file_;
            if(! reader_) {
                status_ = status::err_io_error;
                msg_ = "opening parquet file failed.";
                VLOG_LP(log_error) << msg_;
                error_aborting_ = true;
                return loader_result::running;
            }
        }

        accessor::record_ref ref{};
        if(! reader_->next(ref)) {
            reader_->close();
            reader_.reset();
            continue;
        }

        set_parameter(*ps, ref, mapping_);

        ++running_statement_count_;
        executor::execute_async(
            *db_,
            tx_,
            prepared_,
            std::move(ps),
            nullptr,
            [&](
                status st,
                std::shared_ptr<error::error_info> info,  //NOLINT(performance-unnecessary-value-param)
                std::shared_ptr<request_statistics> stats  //NOLINT(performance-unnecessary-value-param)
            ){
                (void) stats; // TODO
                --running_statement_count_;
                if(st != status::ok) {
                    std::stringstream ss{};
                    ss << "load failed with the statement position:" << records_loaded_ << " status:" << st
                       << " with message \"" << (info ? info->message() : "") << "\"";
                    status_ = st;
                    msg_ = ss.str();
                    VLOG_LP(log_error) << msg_;  //NOLINT
                    error_aborting_ = true;
                    return;
                }
                ++records_loaded_;
            }
        );
    }
    return loader_result::running;
}

std::atomic_size_t& loader::running_statement_count() noexcept {
    return running_statement_count_;
}

std::size_t loader::records_loaded() const noexcept {
    return records_loaded_.load();
}

std::pair<status, std::string> loader::error_info() const noexcept {
    return {status_, msg_};
}

}  // namespace jogasaki::executor::file
