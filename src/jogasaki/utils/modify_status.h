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

#include <jogasaki/kvs/transaction.h>
#include <jogasaki/status.h>
#include <jogasaki/transaction_context.h>

namespace jogasaki::utils {

/**
 * @brief common routine to modify status when status::concurrent_operation is returned
 * @details depending on the configuration, concurrent_operation is modifed to status::not_found or
 *  status::err_serialization_failure (with aborting tx)
 * @param tx transaction
 * @param res status code to modify
 * @param scan whether the call is to modify return code from scan related functions
 */
void modify_concurrent_operation_status(
    transaction_context& tx,
    status& res,
    bool scan
);

/**
 * @see modify_concurrent_operation_status(transaction_context&, status&, bool)
 */
void modify_concurrent_operation_status(
    kvs::transaction& tx,
    status& res,
    bool scan
);

}

