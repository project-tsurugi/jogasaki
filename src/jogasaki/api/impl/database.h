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
#include <yugawara/storage/configurable_provider.h>
#include <yugawara/aggregate/configurable_provider.h>
#include <yugawara/variable/configurable_provider.h>

#include <jogasaki/status.h>
#include <jogasaki/api/database.h>
#include <jogasaki/configuration.h>
#include <jogasaki/api/impl/parameter_set.h>
#include <jogasaki/api/impl/prepared_statement.h>
#include <jogasaki/api/impl/executable_statement.h>
#include <jogasaki/kvs/database.h>

namespace jogasaki::scheduler {
class task_scheduler;
}

namespace jogasaki::api::impl {

using takatori::util::unsafe_downcast;

/**
 * @brief database interface to start/stop the services and initiate transaction requests
 */
class database : public api::database {
public:
    database();

    explicit database(std::shared_ptr<class configuration> cfg);

    [[nodiscard]] status start() override;

    [[nodiscard]] status stop() override;

    status register_variable(std::string_view name, field_type_kind kind) override;

    [[nodiscard]] status prepare(
        std::string_view sql,
        std::unique_ptr<api::prepared_statement>& statement
    ) override;

    [[nodiscard]] status prepare(
        std::string_view sql,
        std::unordered_map<std::string, api::field_type_kind> const& variables,
        std::unique_ptr<api::prepared_statement>& statement
    ) override;

    [[nodiscard]] status create_executable(
        std::string_view sql,
        std::unique_ptr<api::executable_statement>& statement
    ) override;

    [[nodiscard]] status resolve(
        api::prepared_statement const& prepared,
        api::parameter_set const& parameters,
        std::unique_ptr<api::executable_statement>& statement
    ) override;

    [[nodiscard]] status explain(api::executable_statement const& executable, std::ostream& out) override;

    void dump(std::ostream& output, std::string_view index_name, std::size_t batch_size) override;

    void load(std::istream& input, std::string_view index_name, std::size_t batch_size) override;

    std::unique_ptr<api::transaction> do_create_transaction(bool readonly) override;

    [[nodiscard]] std::shared_ptr<class configuration> const& configuration() const noexcept;

    [[nodiscard]] std::shared_ptr<kvs::database> const& kvs_db() const noexcept;

    [[nodiscard]] std::shared_ptr<yugawara::storage::configurable_provider> const& tables() const noexcept;

    [[nodiscard]] std::shared_ptr<yugawara::aggregate::configurable_provider> const&
        aggregate_functions() const noexcept;

    [[nodiscard]] scheduler::task_scheduler* task_scheduler() const noexcept;

protected:
    status do_create_table(
        std::shared_ptr<yugawara::storage::table> table,
        std::string_view schema
    ) override;

    std::shared_ptr<yugawara::storage::table const> do_find_table(
        std::string_view name,
        std::string_view schema
    ) override;

    status do_drop_table(
        std::string_view name,
        std::string_view schema
    ) override;

    status do_create_index(
        std::shared_ptr<yugawara::storage::index> index,
        std::string_view schema
    ) override;

    std::shared_ptr<yugawara::storage::index const> do_find_index(
        std::string_view name,
        std::string_view schema
    ) override;

    status do_drop_index(
        std::string_view name,
        std::string_view schema
    ) override;

    status do_create_sequence(
        std::shared_ptr<yugawara::storage::sequence> sequence,
        std::string_view schema
    ) override;

    std::shared_ptr<yugawara::storage::sequence const> do_find_sequence(
        std::string_view name,
        std::string_view schema
    ) override;

    status do_drop_sequence(
        std::string_view name,
        std::string_view schema
    ) override;

private:
    std::shared_ptr<class configuration> cfg_{};
    std::shared_ptr<yugawara::storage::configurable_provider> tables_{
        std::make_shared<yugawara::storage::configurable_provider>()
    };
    std::shared_ptr<yugawara::aggregate::configurable_provider> aggregate_functions_{
        std::make_shared<yugawara::aggregate::configurable_provider>()
    };
    std::shared_ptr<yugawara::variable::configurable_provider> host_variables_{
        std::make_shared<yugawara::variable::configurable_provider>()
    };
    std::shared_ptr<kvs::database> kvs_db_{};
    std::unique_ptr<scheduler::task_scheduler> task_scheduler_;

    [[nodiscard]] status prepare_common(
        std::string_view sql,
        std::shared_ptr<yugawara::variable::configurable_provider> provider,
        std::unique_ptr<api::prepared_statement>& statement
    );
};

inline api::impl::database& get_impl(api::database& db) {
    return unsafe_downcast<api::impl::database>(db);
}

}

