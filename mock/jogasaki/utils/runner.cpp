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
#include "runner.h"

#include <iostream>
#include <glog/logging.h>

#include <takatori/util/downcast.h>
#include <takatori/util/string_builder.h>

#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/impl/parameter_set.h>
#include <jogasaki/api/impl/record.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/api/record.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/result_set_iterator.h>
#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/api/transaction_handle_internal.h>
#include <jogasaki/error/error_info.h>
#include <jogasaki/executor/executor.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/utils/create_commit_option.h>
#include <jogasaki/utils/create_tx.h>

#define notnull(arg) if (! (arg)) { execution_message_ = "execution failed. " #arg " is null"; return *this; }
#define exec_fail(msg) { execution_message_ = msg; return *this; }

namespace jogasaki::utils {

using takatori::util::unsafe_downcast;
using takatori::util::string_builder;
using jogasaki::api::impl::get_impl;

runner& runner::run() {
    notnull(db_);
    std::shared_ptr<error::error_info> temp{};
    auto* out = output_error_info_ ? output_error_info_ : &temp;

    api::statement_handle prepared{prepared_};
    if(text_is_set_) {
        plan::compile_option opt{};
        opt.session_id(100);
        if(auto res = get_impl(*db_).prepare(
                text_,
                variables_ ? *variables_ : std::unordered_map<std::string, api::field_type_kind>{},
                prepared,
                *out,
                opt
            ); res != status::ok) {
            exec_fail(expect_error_ ? "" : (*out)->message());
        }
    }

    notnull(prepared);
    api::impl::parameter_set empty_params{};
    std::unique_ptr<api::executable_statement> stmt{};
    if(auto res = get_impl(*db_).resolve(
            prepared,
            params_ ? maybe_shared_ptr{params_} : maybe_shared_ptr{&empty_params},
            stmt,
            *out
        ); res != status::ok) {
        exec_fail(expect_error_ ? "" : (*out)->message());
    }
    if(show_plan_ || explain_output_) {
        std::stringstream ss{};
        db_->explain(*stmt, ss);
        if(explain_output_) {
            *explain_output_ = ss.str();
            return *this;
        }
        std::cout << ss.str() << std::endl;
    }

    api::transaction_handle tx{tx_};
    std::shared_ptr<api::transaction_handle> holder{};
    if(! tx) {
        holder = utils::create_transaction(*db_);
        tx = *holder;
    }

    status res{};
    std::shared_ptr<request_statistics> temp_stats{};
    auto* out_stats = stats_ ? stats_ : &temp_stats;
    auto tc = api::get_transaction_context(tx);
    if(output_records_) {
        // call api for query
        std::unique_ptr<api::result_set> rs{};
        if(res = executor::execute(get_impl(*db_), std::move(tc), *stmt, rs, *out, *out_stats); res != status::ok && ! expect_error_) {
            exec_fail(string_builder{} << "execution failed. executor::execute() - " << (*out)->message() << string_builder::to_string);
        }
        auto it = rs->iterator();
        if(show_recs_) {
            LOG(INFO) << "query result : ";
        }
        while(it->has_next()) {
            auto* record = it->next();
            std::stringstream ss{};
            ss << *record;
            auto* rec_impl = unsafe_downcast<api::impl::record>(record);
            auto* meta_impl = unsafe_downcast<api::impl::record_meta>(rs->meta());
            output_records_->emplace_back(rec_impl->ref(), meta_impl->meta());
            if(show_recs_) {
                LOG(INFO) << ss.str();
            }
        }
        rs->close();
    } else {
        // Call api for statement (i.e. no result records).
        // There is no execute() api without requesting result set, so use execute_async with sync = true
        if((! executor::execute_async(
               get_impl(*db_),
               std::move(tc),
               maybe_shared_ptr{stmt.get()},
               nullptr,
               [&](status st, std::shared_ptr<error::error_info> err, std::shared_ptr<request_statistics> stats) {
                   res = st;
                   *out = err;
                   *out_stats = stats;
               },
               request_info{},
               true) ||
             res != status::ok) &&
           ! expect_error_) {
            exec_fail(string_builder{} << "execution failed. executor::execute() - " << (*out)->message() << string_builder::to_string);
        }
    }

    if(! tx_) {
        // commit if tx is generated
        if(! expect_error_) {
            auto* copt = utils::get_global_commit_option();
            if(auto res = tx.commit(*copt); res != status::ok) {
                exec_fail("execution failed. tx.commit()");
            }
        } else {
            if(! no_abort_) {
                if(auto res = tx.abort(); res != status::ok) {
                    exec_fail("execution failed. tx.abort()");
                }
            }
        }
    }

    if(output_status_) {
        *output_status_ = res;
    }
    if(! prepared_) {
        if(auto res = db_->destroy_statement(prepared); res != status::ok) {
            exec_fail("execution failed. db_->destroy_statement()");
        }
    }
    return *this;
}

}
