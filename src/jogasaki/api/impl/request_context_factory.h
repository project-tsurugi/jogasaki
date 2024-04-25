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

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/statement_handle.h>
#include <jogasaki/executor/io/record_channel.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/request_context.h>
#include <jogasaki/scheduler/request_detail.h>
#include <jogasaki/transaction_context.h>

namespace jogasaki::api::impl {

std::shared_ptr<request_context> create_request_context(
    impl::database& db,
    std::shared_ptr<transaction_context> tx,
    maybe_shared_ptr<executor::io::record_channel> const& channel,
    std::shared_ptr<memory::lifo_paged_memory_resource> resource,
    request_info const& req_info,
    std::shared_ptr<scheduler::request_detail> request_detail = nullptr
);

}
