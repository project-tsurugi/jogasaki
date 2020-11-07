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
#include "operator_executor.h"

#include <jogasaki/kvs/database.h>
#include <jogasaki/executor/process/impl/scan_info.h>

#include "scan.h"
#include "scan_context.h"
#include "emit.h"
#include "emit_context.h"
#include "filter.h"
#include "filter_context.h"
#include "project.h"
#include "project_context.h"
#include "take_group.h"
#include "offer.h"
#include "take_flat.h"

namespace jogasaki::executor::process::impl::ops {

namespace relation = takatori::relation;

using takatori::util::fail;
using takatori::relation::step::dispatch;

operator_executor::operator_executor(
    relation::graph_type& relations,
    operator_container* operators,
    abstract::task_context *context,
    memory_resource* resource,
    kvs::database* database
) noexcept :
    relations_(std::addressof(relations)),
    operators_(operators),
    context_(context),
    resource_(resource),
    database_(database),
    root_(operators_ ? &operators_->root() : nullptr)
{}

relation::expression &operator_executor::head() {
    relation::expression* result = nullptr;
    takatori::relation::enumerate_top(*relations_, [&](relation::expression& v) {
        result = &v;
    });
    if (result != nullptr) {
        return *result;
    }
    fail();
}

block_scope& operator_executor::get_block_variables(std::size_t index) {
    return static_cast<work_context *>(context_->work_context())->variables(index); //NOLINT
}

void operator_executor::operator()() {
    static_cast<record_operator*>(root_)->process_record(this);
    // TODO handling status code
}

operator_container &operator_executor::operators() const noexcept {
    return *operators_;
}

context_container &operator_executor::contexts() const noexcept {
    return static_cast<work_context*>(context_->work_context())->container();
}

operator_executor::memory_resource *operator_executor::resource() const noexcept {
    return resource_;
}

kvs::database *operator_executor::database() const noexcept {
    return database_;
}

abstract::task_context *operator_executor::task_context() const noexcept {
    return context_;
}

}


