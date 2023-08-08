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
#include "transaction.h"
#include "executable_statement.h"

#include <takatori/util/downcast.h>
#include <takatori/util/string_builder.h>

#include <jogasaki/constants.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/impl/result_set.h>
#include <jogasaki/plan/compiler.h>
#include <jogasaki/executor/executor.h>
#include <jogasaki/executor/common/execute.h>
#include <jogasaki/executor/common/write.h>
#include <jogasaki/scheduler/flat_task.h>
#include <jogasaki/scheduler/task_scheduler.h>
#include <jogasaki/scheduler/job_context.h>
#include <jogasaki/executor/sequence/sequence.h>
#include <jogasaki/api/impl/result_store_channel.h>
#include <jogasaki/scheduler/task_factory.h>
#include <jogasaki/executor/io/record_channel_adapter.h>
#include <jogasaki/executor/io/dump_channel.h>
#include <jogasaki/executor/io/null_record_channel.h>
#include <jogasaki/utils/backoff_timer.h>
#include <jogasaki/utils/abort_error.h>
#include <jogasaki/utils/hex.h>
#include <jogasaki/accessor/record_printer.h>
#include <jogasaki/index/utils.h>
#include <jogasaki/index/index_accessor.h>
#include <jogasaki/kvs/readable_stream.h>
#include <jogasaki/request_logging.h>

#include "request_context_factory.h"
#include "jogasaki/index/field_factory.h"

namespace jogasaki::api::impl {

using takatori::util::unsafe_downcast;
using takatori::util::string_builder;

constexpr static std::string_view log_location_prefix = "/:jogasaki:api:impl:transaction ";

status transaction::commit() {
    return executor::commit(*database_, tx_);
}

status transaction::commit_internal() {
    return executor::commit_internal(*database_, tx_);
}

status transaction::abort() {
    return executor::abort(*database_, tx_);
}

status transaction::execute(
    api::executable_statement& statement,
    std::unique_ptr<api::result_set>& result
) {
    return executor::execute(*database_, tx_, statement, result);
}

impl::database& transaction::database() {
    return *database_;
}

transaction::transaction(
    impl::database& database
) :
    database_(std::addressof(database))
{}

status transaction::execute(
    api::statement_handle prepared,
    std::shared_ptr<api::parameter_set> parameters,
    std::unique_ptr<api::result_set>& result
) {
    return executor::execute(
        *database_,
        tx_,
        prepared,
        std::move(parameters),
        result
    );
}

bool transaction::execute_async(
    api::statement_handle prepared,
    std::shared_ptr<api::parameter_set> parameters,
    maybe_shared_ptr<executor::io::record_channel> const& channel,
    callback on_completion,
    bool sync
) {
    return executor::execute_async(
        *database_,
        tx_,
        prepared,
        std::move(parameters),
        channel,
        std::move(on_completion),
        sync
    );
}

bool transaction::execute_async(
    maybe_shared_ptr<api::executable_statement> const& statement,
    maybe_shared_ptr<api::data_channel> const& channel,
    callback on_completion  //NOLINT(performance-unnecessary-value-param)
) {
    return executor::execute_async(
        *database_,
        tx_,
        statement,
        channel,
        std::move(on_completion)
    );
}

bool transaction::execute_dump(
    maybe_shared_ptr<api::executable_statement> const& statement,
    maybe_shared_ptr<api::data_channel> const& channel,
    std::string_view directory,
    callback on_completion,
    std::size_t max_records_per_file,
    bool keep_files_on_error
) {
    return executor::execute_dump(
        *database_,
        tx_,
        statement,
        channel,
        directory,
        std::move(on_completion),
        max_records_per_file,
        keep_files_on_error
    );
}

/*
std::shared_ptr<request_context> transaction::create_request_context(
    maybe_shared_ptr<executor::io::record_channel> const& channel,
    std::shared_ptr<memory::lifo_paged_memory_resource> resource,
    std::shared_ptr<scheduler::request_detail> request_detail
) {
    return executor::create_request_context(
        *database_,
        tx_,
        channel,
        std::move(resource),
        std::move(request_detail)
    );
}


bool transaction::execute_internal(
    maybe_shared_ptr<api::executable_statement> const& statement,
    maybe_shared_ptr<executor::io::record_channel> const& channel,
    callback on_completion, //NOLINT(performance-unnecessary-value-param)
    bool sync
) {
    return executor::execute_internal(
        *database_,
        tx_,
        statement,
        channel,
        std::move(on_completion),
        sync
    );
}
 */

bool transaction::execute_async_on_context(
    std::shared_ptr<request_context> rctx,  //NOLINT
    maybe_shared_ptr<api::executable_statement> const& statement,
    callback on_completion, //NOLINT(performance-unnecessary-value-param)
    bool sync
) {
    return executor::execute_async_on_context(
        *database_,
        tx_,
        std::move(rctx),
        statement,
        std::move(on_completion),
        sync
    );
}

bool transaction::execute_load(
    api::statement_handle prepared,
    maybe_shared_ptr<api::parameter_set const> parameters,
    std::vector<std::string> files,
    transaction::callback on_completion
) {
    return executor::execute_load(
        *database_,
        tx_,
        prepared,
        parameters,
        std::move(files),
        std::move(on_completion)
    );
}

scheduler::job_context::job_id_type transaction::commit_async(
    transaction::callback on_completion
) {
    return executor::commit_async(
        *database_,
        tx_,
        std::move(on_completion)
    );
}

bool transaction::transaction::is_ready() const {
    return executor::is_ready(
        *database_,
        tx_
    );
}

std::string_view transaction::transaction_id() const noexcept {
    return executor::transaction_id(
        *database_,
        tx_
    );
}

std::shared_ptr<transaction_context> const &transaction::context() const noexcept {
    return tx_;
}

}
