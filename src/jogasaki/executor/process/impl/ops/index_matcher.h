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
#pragma once

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <takatori/relation/join_find.h>
#include <takatori/relation/join_scan.h>
#include <takatori/scalar/expression.h>
#include <takatori/tree/tree_fragment_vector.h>
#include <takatori/util/optional_ptr.h>
#include <takatori/util/sequence_view.h>
#include <yugawara/storage/index.h>

#include <jogasaki/data/aligned_buffer.h>
#include <jogasaki/executor/expr/evaluator.h>
#include <jogasaki/executor/process/abstract/task_context.h>
#include <jogasaki/executor/process/impl/ops/context_helper.h>
#include <jogasaki/executor/process/impl/ops/details/error_abort.h>
#include <jogasaki/executor/process/impl/ops/details/expression_error.h>
#include <jogasaki/executor/process/impl/ops/operation_status.h>
#include <jogasaki/executor/process/impl/ops/operator_kind.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/index/field_factory.h>
#include <jogasaki/index/field_info.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/kvs/iterator.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/status.h>
#include <jogasaki/transaction_context.h>
#include <jogasaki/utils/handle_encode_errors.h>
#include <jogasaki/utils/handle_generic_error.h>
#include <jogasaki/utils/handle_kvs_errors.h>
#include <jogasaki/utils/make_function_context.h>
#include <jogasaki/utils/modify_status.h>

#include "details/encode_key.h"
#include "details/search_key_field_info.h"
#include "index_field_mapper.h"
#include "index_join_context.h"
#include "operator_base.h"

namespace jogasaki::executor::process::impl::ops::details {

/**
 * @brief static info. for matcher to do join_scan operation
 */
class match_info_scan {
public:
    match_info_scan() = default;

    match_info_scan(
        std::vector<details::search_key_field_info> begin_fields,
        kvs::end_point_kind begin_endpoint,
        std::vector<details::search_key_field_info> end_fields,
        kvs::end_point_kind end_endpoint,
        std::vector<details::secondary_index_field_info> secondary_key_fields
    ) :
        begin_fields_(std::move(begin_fields)),
        begin_endpoint_(begin_endpoint),
        end_fields_(std::move(end_fields)),
        end_endpoint_(end_endpoint),
        secondary_key_fields_(std::move(secondary_key_fields))
    {}

    std::vector<details::search_key_field_info> begin_fields_{};  //NOLINT
    kvs::end_point_kind begin_endpoint_{};  //NOLINT
    std::vector<details::search_key_field_info> end_fields_{};  //NOLINT
    kvs::end_point_kind end_endpoint_{};  //NOLINT
    std::vector<details::secondary_index_field_info> secondary_key_fields_{};  //NOLINT
};

/**
 * @brief static info. for matcher to do join_find operation
 */
class match_info_find {
public:

    match_info_find(
        std::vector<details::search_key_field_info> key_fields,
        std::vector<details::secondary_index_field_info> secondary_key_fields
    ) :
        key_fields_(std::move(key_fields)),
        secondary_key_fields_(std::move(secondary_key_fields))
    {}

    std::vector<details::search_key_field_info> key_fields_{};  //NOLINT
    std::vector<details::secondary_index_field_info> secondary_key_fields_{};  //NOLINT
};

/**
 * @brief matcher object to conduct matching of input record and index keys
 * @details this object encapsulates difference between single result and multiple result
 */
template <class MatchInfo>
class matcher {
public:
    using memory_resource = memory::lifo_paged_memory_resource;

    matcher(
        bool use_secondary,
        MatchInfo const& info,
        std::vector<index::field_info> key_columns,
        std::vector<index::field_info> value_columns
    ) :
        use_secondary_(use_secondary),
        info_(info),
        for_join_scan_(std::is_same_v<MatchInfo, match_info_scan>),
        field_mapper_(
            use_secondary_,
            std::move(key_columns),
            std::move(value_columns),
            info.secondary_key_fields_
        )
    {}

    /**
     * @brief execute the matching for join_find
     * @return true if match is successful (i.e. matching record is found and variables are filled)
     * @return false if match is not successful. Check status with result() function to see if the result is
     * simply not-found or other error happened. When other error happens than status::not_found, `ctx` is populated with error info.
     * Typical return values are as follows.
     * - status::not_found: a matching successfully completed and there are no more records to process
     * - status::err_serialization_failure: an error occurred while reading key or value (cc serialization error or
     * concurrent operation)
     */
    template <class T = MatchInfo>
    [[nodiscard]] std::enable_if_t<std::is_same_v<T, match_info_find>, bool> process(
        request_context& ctx,
        variable_table& input_variables,
        variable_table& output_variables,
        kvs::storage& primary_stg,
        kvs::storage* secondary_stg,
        memory_resource* resource = nullptr
    ) {
        std::size_t len{};
        std::string msg{};
        if(auto res =
               details::encode_key(std::addressof(ctx), info_.key_fields_, input_variables, *resource, buf_, len, msg);
           res != status::ok) {
            status_ = res;
            if(res == status::err_integrity_constraint_violation) {
                // null is assigned for find condition. Nothing should match.
                status_ = status::not_found;
                return false;
            }
            handle_encode_errors(ctx, res);
            handle_generic_error(ctx, res, error_code::sql_execution_exception);
            return false;
        }
        std::string_view key{static_cast<char*>(buf_.data()), len};
        std::string_view value{};

        if (! use_secondary_) {
            auto res = primary_stg.content_get(*ctx.transaction(), key, value);
            status_ = res;
            if (res != status::ok) {
                utils::modify_concurrent_operation_status(*ctx.transaction(), res, false);
                status_ = res;
                if(res == status::not_found) {
                    return false;
                }
                handle_kvs_errors(ctx, res);
                handle_generic_error(ctx, res, error_code::sql_execution_exception);
                return false;
            }
            return field_mapper_.process(
                       key,
                       value,
                       output_variables.store().ref(),
                       primary_stg,
                       *ctx.transaction(),
                       resource,
                       ctx
                   ) == status::ok;
        }
        // handle secondary index
        if(auto res = secondary_stg->content_scan(
               *ctx.transaction(),
               key,
               kvs::end_point_kind::prefixed_inclusive,
               key,
               kvs::end_point_kind::prefixed_inclusive,
               it_
           );
           res != status::ok) {
            // content_scan does not return not_found or concurrent_operation
            status_ = res;
            handle_kvs_errors(ctx, res);
            handle_generic_error(ctx, res, error_code::sql_execution_exception);
            return false;
        }

        // remember parameters for current scan
        output_variables_ = std::addressof(output_variables);
        primary_storage_ = std::addressof(primary_stg);
        tx_ = std::addressof(*ctx.transaction());
        resource_ = resource;
        return next(ctx);
    }

    /**
     * @brief execute the matching for join_scan
     * @return true if match is successful (i.e. matching record is found and variables are filled)
     * @return false if match is not successful. Check status with result() function to see if the result is
     * simply not-found or other error happened. When other error happens than status::not_found, `ctx` is populated with error info.
     * Typical return values are as follows.
     * - status::not_found: a matching successfully completed and there are no more records to process
     * - status::err_serialization_failure: an error occurred while reading key or value (cc serialization error or
     * concurrent operation)
     */
    template <class T = MatchInfo>
    [[nodiscard]] std::enable_if_t<std::is_same_v<T, match_info_scan>, bool> process(
        request_context& ctx,
        variable_table& input_variables,
        variable_table& output_variables,
        kvs::storage& primary_stg,
        kvs::storage* secondary_stg,
        memory_resource* resource = nullptr
    ) {
        std::size_t begin_len{};
        std::size_t end_len{};
        std::string msg{};
        if(auto res = details::encode_key(std::addressof(ctx), info_.begin_fields_, input_variables, *resource, buf_, begin_len, msg);
        res != status::ok) {
            status_ = res;
            // TODO handle msg
            if (res == status::err_integrity_constraint_violation) {
                // null is assigned for find condition. Nothing should match.
                status_ = status::not_found;
                return false;
            }
            handle_encode_errors(ctx, res);
            handle_generic_error(ctx, res, error_code::sql_execution_exception);
            return false;
        }
        if(auto res = details::encode_key(std::addressof(ctx), info_.end_fields_, input_variables, *resource, buf2_, end_len, msg);
        res != status::ok) {
            status_ = res;
            // TODO handle msg
            if (res == status::err_integrity_constraint_violation) {
                // null is assigned for find condition. Nothing should match.
                status_ = status::not_found;
                return false;
            }
            handle_encode_errors(ctx, res);
            handle_generic_error(ctx, res, error_code::sql_execution_exception);
            return false;
        }
        std::string_view b{static_cast<char*>(buf_.data()), begin_len};
        std::string_view e{static_cast<char*>(buf2_.data()), end_len};

        auto& stg = use_secondary_ ? *secondary_stg : primary_stg;
        if(auto res = stg.content_scan(*ctx.transaction(),
                b, info_.begin_endpoint_,
                e, info_.end_endpoint_,
                it_
            ); res != status::ok) {
                // content_scan does not return not_found or concurrent_operation
                status_ = res;
                handle_kvs_errors(ctx, res);
                handle_generic_error(ctx, res, error_code::sql_execution_exception);
                return false;
        }

        // remember parameters for current scan
        output_variables_ = std::addressof(output_variables);
        primary_storage_ = std::addressof(primary_stg);
        tx_ = std::addressof(*ctx.transaction());
        resource_ = resource;
        return next(ctx);
    }


    /**
     * @brief retrieve next match
     * @param ctx request context
     * @return true if match is successful
     * @return false if match is not successful, check status with result() function to see if the result is
     * simply not-found or other error happened.
     * When other error happens, `ctx` is populated with error info.
     */
    bool next(request_context& ctx) {
        if (it_ == nullptr) {
            status_ = status::not_found;
            return false;
        }
        while(true) {  // loop to skip not_found with key()/value()
            auto res = it_->next();
            if(res != status::ok) {
                // next() does not return concurrent_operation, so no need to call modify_concurrent_operation_status
                status_ = res;
                if(res != status::not_found) {
                    handle_kvs_errors(ctx, res);
                    handle_generic_error(ctx, res, error_code::sql_execution_exception);
                }
                it_.reset();
                return false;
            }
            std::string_view key{};
            std::string_view value{};
            if(auto r = it_->read_key(key); r != status::ok) {
                utils::modify_concurrent_operation_status(*tx_, r, true);
                if(r == status::not_found) {
                    continue;
                }
                status_ = r;
                handle_kvs_errors(ctx, r);
                handle_generic_error(ctx, r, error_code::sql_execution_exception);
                it_.reset();
                return false;
            }
            if(auto r = it_->read_value(value); r != status::ok) {
                utils::modify_concurrent_operation_status(*tx_, r, true);
                if(r == status::not_found) {
                    continue;
                }
                status_ = r;
                handle_kvs_errors(ctx, r);
                handle_generic_error(ctx, r, error_code::sql_execution_exception);
                it_.reset();
                return false;
            }
            return field_mapper_.process(
                       key,
                       value,
                       output_variables_->store().ref(),
                       *primary_storage_,
                       *tx_,
                       resource_,
                       ctx
                   ) == status::ok;
        }
    }


    /**
     * @brief retrieve the status code of the last match execution
     * @details This function provides the status code to check if the match
     * @return status::ok if match was successful
     * @return status::not_found if match was not successful due to missing target record
     * @return other error (e.g. status::err_aborted_retryable) occurred when accessing kvs
     */
    [[nodiscard]] status result() const noexcept {
        return status_;
    }

private:
    bool use_secondary_{};
    MatchInfo const& info_;
    bool for_join_scan_{};
    data::aligned_buffer buf_{};
    data::aligned_buffer buf2_{};
    status status_{status::ok};
    index_field_mapper field_mapper_{};

    variable_table* output_variables_{};
    kvs::storage* primary_storage_{};
    transaction_context* tx_{};
    matcher::memory_resource* resource_{};
    std::unique_ptr<kvs::iterator> it_{};
};

}  // namespace jogasaki::executor::process::impl::ops::details
