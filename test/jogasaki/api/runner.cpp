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
#include "runner.h"

#include <gtest/gtest.h>
#include <takatori/util/downcast.h>

#include <jogasaki/api/impl/record.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/api/impl/database.h>

#include <jogasaki/utils/create_tx.h>

#define notnull(arg) if (! (arg)) { execution_message_ = "execution failed. " #arg " is null"; return *this; }
#define exec_fail(msg) { execution_message_ = msg; return *this; }

namespace jogasaki::testing {

using takatori::util::unsafe_downcast;

runner& runner::run() {
    notnull(db_);
    api::statement_handle prepared{prepared_};
    if(! text_.empty()) {
        if(auto res = db_->prepare(
                text_,
                variables_ ? *variables_ : std::unordered_map<std::string, api::field_type_kind>{},
                prepared
            ); res != status::ok) {
            exec_fail("execution failed. db_->prepare()");
        }
    }

    notnull(prepared);
    api::impl::parameter_set empty_params{};
    std::unique_ptr<api::executable_statement> stmt{};
    if(auto res = db_->resolve(
            prepared,
            params_ ? maybe_shared_ptr{params_} : maybe_shared_ptr{&empty_params},
            stmt
        ); res != status::ok) {
        exec_fail("execution failed. db_->resolve()");
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
    if(output_records_) {
        std::unique_ptr<api::result_set> rs{};
        if(auto res = tx.execute(*stmt, rs);res != status::ok) {
            exec_fail("execution failed. tx.execute()");
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
        res = tx.execute(*stmt);
    }

    if(! tx_) {
        // commit if tx is generated
        if(! expect_error_) {
            if(auto res = tx.commit(); res != status::ok) {
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
