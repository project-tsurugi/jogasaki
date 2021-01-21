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
#pragma once

#include <string_view>
#include <memory>

#include <takatori/util/downcast.h>

#include <jogasaki/api/database.h>
#include <jogasaki/configuration.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/executor/common/execute.h>
#include <jogasaki/executor/common/write.h>
#include <jogasaki/executor/function/builtin_functions.h>
#include <jogasaki/plan/compiler.h>
#include <jogasaki/scheduler/statement_scheduler.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/api/impl/parameter_set.h>
#include <jogasaki/api/impl/prepared_statement.h>
#include <jogasaki/api/impl/executable_statement.h>

namespace jogasaki::api::impl {

using takatori::util::unsafe_downcast;

class transaction;

/**
 * @brief database interface to start/stop the services and initiate transaction requests
 */
class database : public api::database {
public:
    database();
    explicit database(std::shared_ptr<configuration> cfg);

    [[nodiscard]] bool start() override;

    [[nodiscard]] bool stop() override;

    [[nodiscard]] bool prepare(std::string_view sql, std::unique_ptr<api::prepared_statement>& statement) override;

    [[nodiscard]] bool create_executable(std::string_view sql, std::unique_ptr<api::executable_statement>& statement) override;

    [[nodiscard]] bool resolve(
        api::prepared_statement const& prepared,
        api::parameter_set const& parameters,
        std::unique_ptr<api::executable_statement>& statement
    ) override;

    [[nodiscard]] bool explain(api::executable_statement const& executable, std::ostream& out) override {
        (void)executable;
        (void)out;
        return true;
    }

    std::unique_ptr<api::transaction> create_transaction() override;

    [[nodiscard]] std::shared_ptr<class configuration> const& configuration() const noexcept;
    [[nodiscard]] static database* get_impl(api::database& arg) noexcept;

    [[nodiscard]] std::shared_ptr<kvs::database> const& kvs_db() const noexcept;

    [[nodiscard]] std::shared_ptr<yugawara::storage::configurable_provider> const& tables() const noexcept;

    [[nodiscard]] std::shared_ptr<yugawara::aggregate::configurable_provider> const& aggregate_functions() const noexcept;
private:
    std::shared_ptr<class configuration> cfg_{};
    std::shared_ptr<yugawara::storage::configurable_provider> tables_{std::make_shared<yugawara::storage::configurable_provider>()};
    std::shared_ptr<yugawara::aggregate::configurable_provider> aggregate_functions_{std::make_shared<yugawara::aggregate::configurable_provider>()};
    std::shared_ptr<kvs::database> kvs_db_{};
};

}

