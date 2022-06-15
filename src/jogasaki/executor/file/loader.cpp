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
#include "loader.h"

#include <atomic>
#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/fail.h>

#include <jogasaki/logging.h>
#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/transaction.h>
#include <jogasaki/api/impl/parameter_set.h>
#include <jogasaki/api/impl/prepared_statement.h>

namespace jogasaki::executor::file {

using takatori::util::maybe_shared_ptr;
using takatori::util::fail;

loader::loader(
    std::vector<std::string> files,
    api::statement_handle prepared,
    maybe_shared_ptr<api::parameter_set const> parameters,
    api::impl::transaction* tx,
    std::size_t bulk_size
) noexcept:
    files_(std::move(files)),
    prepared_(prepared),
    parameters_(std::move(parameters)),
    tx_(tx),
    next_file_(files_.begin()),
    bulk_size_(bulk_size)
{}

meta::field_type_kind host_variable_type(executor::process::impl::variable_table_info const& vinfo, std::string_view name) {
    if (! vinfo.exists(name)) {
        fail();
    }
    auto idx = vinfo.at(name).index();
    return vinfo.meta()->at(idx).kind();
}

std::unordered_map<std::string, parameter> create_mapping(
    api::parameter_set const& ps,
    api::statement_handle prepared,
    meta::external_record_meta const& meta
) {
    std::unordered_map<std::string, parameter> ret{};
    auto& impl = static_cast<api::impl::parameter_set const&>(ps);  //NOLINT
    auto& body = impl.body();

    auto stmt = reinterpret_cast<api::impl::prepared_statement*>(prepared.get()); //NOLINT
    auto vinfo = stmt->body()->mirrors()->host_variable_info();

    ret.reserve(body->size());
    for(auto&& [name, e ] : *body) {
        if(e.type().kind() == meta::field_type_kind::reference_column_position) {
            auto idx = e.as_any().to<std::size_t>();
            ret[name] = parameter{
                host_variable_type(*vinfo, name),
                idx,
                meta.value_offset(idx),
                meta.nullity_offset(idx)
            };
            continue;
        }
        if(e.type().kind() == meta::field_type_kind::reference_column_name) {
            auto t = e.as_any().to<accessor::text>();
            auto referenced = static_cast<std::string_view>(t);
            auto idx = meta.field_index(referenced);
            if(idx == meta::external_record_meta::undefined) {
                fail();
            }
            ret[name] = parameter{
                host_variable_type(*vinfo, name),
                idx,
                meta.value_offset(idx),
                meta.nullity_offset(idx)
            };
            continue;
        }
    }
    return ret;
}

void set_parameter(api::parameter_set& ps, accessor::record_ref ref, std::unordered_map<std::string, parameter> const& mapping) {
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
            default: fail();
        }
    }
}

bool loader::operator()() {
    if (! more_to_read_) {
        return running_statement_count_ != 0;
    }
    auto slots = bulk_size_ - running_statement_count_;
    if (slots == 0) {
        return true;
    }
    for(std::size_t i=0; i < slots; ++i) {
        // read records, assign host variables, submit tasks
        auto ps = std::shared_ptr<api::parameter_set>{parameters_->clone()};
        if(! reader_) {
            if(next_file_ == files_.end()) {
                // reading all files completed
                more_to_read_ = false;
                return running_statement_count_ != 0;
            }
            reader_ = parquet_reader::open(*next_file_);
            ++next_file_;
            if(! reader_) {
                // error handling TODO
                VLOG(log_error) << "opening parquet file failed.";
                return false;
            }
        }

        if (! meta_) {
            meta_ = reader_->meta();
            mapping_ = create_mapping(*parameters_, prepared_, *meta_);
        }

        accessor::record_ref ref{};
        if(! reader_->next(ref)) {
            reader_->close();
            reader_.reset();
            continue;
        }

        set_parameter(*ps, ref, mapping_);

        ++running_statement_count_;
        tx_->execute_async(prepared_,
            std::move(ps),
            nullptr,
            [&](status st, std::string_view msg){
                (void)st;
                (void)msg;
                --running_statement_count_;
                ++records_loaded_;
                // TODO error handling
            }
        );
    }
    return true;
}

std::atomic_size_t& loader::running_statement_count() noexcept {
    return running_statement_count_;
}

std::size_t loader::records_loaded() const noexcept {
    return records_loaded_.load();
}


}

