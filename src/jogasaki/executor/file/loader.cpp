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

#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/transaction.h>
#include <jogasaki/api/impl/parameter_set.h>

namespace jogasaki::executor::file {

using takatori::util::maybe_shared_ptr;
using takatori::util::fail;

loader::loader(
    std::vector<std::string> files,
    request_context* rctx,
    api::statement_handle prepared,
    maybe_shared_ptr<api::parameter_set const> parameters,
    api::database* db,
    api::impl::transaction* tx
) noexcept:
    files_(std::move(files)),
    rctx_(rctx),
    prepared_(prepared),
    parameters_(std::move(parameters)),
    db_(db),
    tx_(tx),
    next_file_(files_.begin())
{}

struct parameter {
    meta::field_type_kind type_{};
    std::size_t index_{};
};

void set_parameter(api::parameter_set& ps, accessor::record_ref ref, std::unordered_map<std::string, parameter> const& mapping) {
    auto& impl = static_cast<api::impl::parameter_set&>(ps);
    auto body = impl.body();
    for(auto&& [name, param] : mapping) {
        if(ref.is_null(param.index_)) {
            body->set_null(name);
            continue;
        }
        using kind = meta::field_type_kind;
        switch(param.type_) {
            case meta::field_type_kind::int4: body->set_int4(name, ref.get_value<runtime_t<kind::int4>>(param.index_)); break;
            case meta::field_type_kind::int8: body->set_int8(name, ref.get_value<runtime_t<kind::int8>>(param.index_)); break;
            case meta::field_type_kind::float4: body->set_float4(name, ref.get_value<runtime_t<kind::float4>>(param.index_)); break;
            case meta::field_type_kind::float8: body->set_float8(name, ref.get_value<runtime_t<kind::float8>>(param.index_)); break;
            case meta::field_type_kind::character: body->set_character(name, ref.get_value<runtime_t<kind::character>>(param.index_)); break;
            default: fail();
        }
    }
}

bool loader::operator()() {
    auto slots = bulk_size - running_statements_;
    if (slots > 0) {
        ++count_;
        for(std::size_t i=0; i < slots; ++i) {
            // read records, assign host variables, submit tasks
            auto ps = std::shared_ptr<api::parameter_set>{parameters_->clone()};
            if(! reader_ && next_file_ != files_.end()) {
                reader_ = parquet_reader::open(*next_file_);
                ++next_file_;
                if(! reader_) {
                    // error handling TODO
                    fail();
                }
            }

            accessor::record_ref ref{};
            if(! reader_->next(ref)) {
                reader_->close();
                reader_.reset();
                continue;
            }

            set_parameter(*ps, ref, {});

            tx_->execute_async(prepared_,
                std::move(ps),
                nullptr,
                [&](status st, std::string_view msg){
                    (void)st;
                    (void)msg;
                    --running_statements_;
                    // TODO error handling
                }
            );
        }
    }
    return true;
}



}

