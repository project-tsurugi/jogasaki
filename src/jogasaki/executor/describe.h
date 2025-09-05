/*
 * Copyright 2018-2025 Project Tsurugi.
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

#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/api/commit_option.h>
#include <jogasaki/api/data_channel.h>
#include <jogasaki/api/executable_statement.h>
#include <jogasaki/api/parameter_set.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/statement_handle.h>
#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/error/error_info.h>
#include <jogasaki/executor/io/dump_config.h>
#include <jogasaki/executor/io/record_channel.h>
#include <jogasaki/kvs/transaction_option.h>
#include <jogasaki/request_context.h>
#include <jogasaki/request_info.h>
#include <jogasaki/request_statistics.h>
#include <jogasaki/scheduler/flat_task.h>
#include <jogasaki/scheduler/job_context.h>
#include <jogasaki/scheduler/statement_scheduler.h>
#include <jogasaki/status.h>
#include <jogasaki/transaction_context.h>
#include <jogasaki/utils/latch.h>

namespace jogasaki::executor {

/**
 * @brief result object of describe table
 */
class describe_result {
public:
	/**
	 * @brief creates a new empty instance.
	 */
	describe_result() = default;

	/**
	 * @brief creates a new object.
	 * @param other the copy source
	 */
	describe_result(describe_result const&) = default;

	/**
	 * @brief creates a new object from the move source.
	 */
	describe_result(describe_result&&) noexcept = default;

	/**
	 * @brief assigns the given object.
	 * @param other the copy source
	 * @return this
	 */
	describe_result& operator=(describe_result const&) = default;

	/**
	 * @brief assigns the given object from the move source.
	 * @param other the move source
	 * @return this
	 */
	describe_result& operator=(describe_result&&) noexcept = default;

	/**
	 * @brief destroys this object.
	 */
	~describe_result() = default;

private:

};

/**
 * @brief describe table
 * @param table_name the target table name
 * @param out the output result, valid only if the return status is status::ok
 * @param error the error info, valid only if the return status is not status::ok
 * @param req_info exchange the original request/response info (mainly for logging purpose)
 * @return status::ok if succeeded, otherwise error status
 * @return status::err_illegal_operation if the authorization is not sufficient
 */
status describe(
    std::string_view table_name,
    describe_result& out,
    std::shared_ptr<error::error_info>& error,
    request_info const& req_info = {}
);

}  // namespace jogasaki::executor

