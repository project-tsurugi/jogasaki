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
#include <tbb/concurrent_hash_map.h>

#include <takatori/util/downcast.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <yugawara/storage/configurable_provider.h>
#include <yugawara/aggregate/configurable_provider.h>
#include <yugawara/variable/configurable_provider.h>

#include <jogasaki/status.h>
#include <jogasaki/api/database.h>
#include <jogasaki/configuration.h>
#include <jogasaki/api/statement_handle.h>
#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/api/impl/parameter_set.h>
#include <jogasaki/api/impl/prepared_statement.h>
#include <jogasaki/api/impl/executable_statement.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/transaction_context.h>
#include <jogasaki/scheduler/job_context.h>
#include <jogasaki/executor/sequence/manager.h>
#include <jogasaki/executor/sequence/sequence.h>
#include <jogasaki/proto/metadata/storage.pb.h>

#include <tateyama/status.h>

namespace jogasaki::scheduler {
class task_scheduler;
}

namespace jogasaki::api::impl {

using takatori::util::unsafe_downcast;
using takatori::util::maybe_shared_ptr;

/**
 * @brief database interface to start/stop the services and initiate transaction requests
 */
class database : public api::database {
public:
    using callback = std::function<void(status, std::shared_ptr<error::error_info>)>;

    using create_transaction_callback = api::database::create_transaction_callback;

    /**
     * @brief callback type for create transaction passing error information
     */
    using create_transaction_callback_error_info = std::function<void(transaction_handle, status, std::shared_ptr<api::error_info>)>;

    database();

    /**
     * @brief create new object with new kvs instance (sharksfin)
     * @param cfg the database configuration
     * @details the newly created kvs instance is owned and managed by this object.
     * Its life-cycle management functions such as open, close, and dispose will be called from this object.
     */
    explicit database(std::shared_ptr<class configuration> cfg);

    /**
     * @brief create new object with existing kvs instance (sharksfin)
     * @param cfg the database configuration
     * @param db sharksfin database handle already opened.
     * @details the existing opened kvs database instance is borrowed and its reference is held by this object.
     * Its life-cycle management functions such as open, close, and dispose will *NOT* be called from this object.
     */
    database(std::shared_ptr<class configuration> cfg, sharksfin::DatabaseHandle db);

    [[nodiscard]] status start() override;

    [[nodiscard]] status stop() override;

    [[nodiscard]] status prepare(
        std::string_view sql,
        api::statement_handle& statement
    ) override;

    [[nodiscard]] status prepare(
        std::string_view sql,
        api::statement_handle& statement,
        std::shared_ptr<error::error_info>& out
    );

    [[nodiscard]] status prepare(
        std::string_view sql,
        std::unordered_map<std::string, api::field_type_kind> const& variables,
        api::statement_handle& statement
    ) override;

    [[nodiscard]] status prepare(
        std::string_view sql,
        std::unordered_map<std::string, api::field_type_kind> const& variables,
        api::statement_handle& statement,
        std::shared_ptr<error::error_info>& out
    );

    [[nodiscard]] status create_executable(
        std::string_view sql,
        std::unique_ptr<api::executable_statement>& statement
    ) override;

    [[nodiscard]] status create_executable(
        std::string_view sql,
        std::unique_ptr<api::executable_statement>& statement,
        std::shared_ptr<error::error_info>& out
    );

    [[nodiscard]] status resolve(
        api::statement_handle prepared,
        maybe_shared_ptr<api::parameter_set const> parameters,
        std::unique_ptr<api::executable_statement>& statement
    ) override;

    [[nodiscard]] status resolve(
        api::statement_handle prepared,
        maybe_shared_ptr<api::parameter_set const> parameters,
        std::unique_ptr<api::executable_statement>& statement,
        std::shared_ptr<error::error_info>& out
    );

    [[nodiscard]] status destroy_statement(
        api::statement_handle prepared
    ) override;

    [[nodiscard]] status destroy_transaction(
        api::transaction_handle handle
    ) override;

    [[nodiscard]] std::size_t transaction_count() const;

    [[nodiscard]] status explain(api::executable_statement const& executable, std::ostream& out) override;

    status dump(std::ostream& output, std::string_view index_name, std::size_t batch_size) override;

    status load(std::istream& input, std::string_view index_name, std::size_t batch_size) override;

    status do_create_transaction(transaction_handle& handle, transaction_option const& option) override;

    scheduler::job_context::job_id_type do_create_transaction_async(
        create_transaction_callback on_completion,
        transaction_option const& option
    ) override;

    scheduler::job_context::job_id_type do_create_transaction_async(
        create_transaction_callback_error_info on_completion,
        transaction_option const& option
    );

    [[nodiscard]] std::shared_ptr<class configuration> const& configuration() const noexcept;

    [[nodiscard]] std::shared_ptr<kvs::database> const& kvs_db() const noexcept;

    [[nodiscard]] std::shared_ptr<yugawara::storage::configurable_provider> const& tables() const noexcept;

    [[nodiscard]] std::shared_ptr<yugawara::aggregate::configurable_provider> const&
        aggregate_functions() const noexcept;

    [[nodiscard]] scheduler::task_scheduler* task_scheduler() const noexcept;

    [[nodiscard]] executor::sequence::manager* sequence_manager() const noexcept;

    status initialize_from_providers();

    [[nodiscard]] std::shared_ptr<scheduler::task_scheduler> const& scheduler() const noexcept;

    std::shared_ptr<class configuration>& config() noexcept override;

    void init();
    void deinit();

    status recover_metadata();
    status recover_table(proto::metadata::storage::IndexDefinition const& idef);
    status recover_index_metadata(
        std::vector<std::string> const& keys,
        bool primary_only,
        std::vector<std::string>& skipped
    );

    void print_diagnostic(std::ostream& os) override;

    std::string diagnostic_string() override;

    status list_tables(std::vector<std::string>& out) override;

    status list_tables(
        std::vector<std::string>& out,
        std::shared_ptr<error::error_info>& err_info
    );

    bool execute_load(
        api::statement_handle prepared,
        maybe_shared_ptr<api::parameter_set const> parameters,
        std::vector<std::string> files,
        callback on_completion
    );

    [[nodiscard]] std::shared_ptr<transaction_context> find_transaction(api::transaction_handle handle);

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
    std::shared_ptr<kvs::database> kvs_db_{};
    std::shared_ptr<scheduler::task_scheduler> task_scheduler_;
    std::unique_ptr<executor::sequence::manager> sequence_manager_{};
    tbb::concurrent_hash_map<api::statement_handle, std::unique_ptr<impl::prepared_statement>> prepared_statements_{};
    tbb::concurrent_hash_map<api::transaction_handle, std::shared_ptr<transaction_context>> transactions_{};
    bool initialized_{false};

    [[nodiscard]] status prepare_common(
        std::string_view sql,
        std::shared_ptr<yugawara::variable::configurable_provider> provider,
        std::unique_ptr<impl::prepared_statement>& statement,
        std::shared_ptr<error::error_info>& out
    );

    [[nodiscard]] status prepare_common(
        std::string_view sql,
        std::shared_ptr<yugawara::variable::configurable_provider> provider,
        api::statement_handle& statement,
        std::shared_ptr<error::error_info>& out
    );

    [[nodiscard]] status resolve_common(
        impl::prepared_statement const& prepared,
        maybe_shared_ptr<api::parameter_set const> parameters,
        std::unique_ptr<api::executable_statement>& statement,
        std::shared_ptr<error::error_info>& out
    );

    status validate_option(transaction_option const& option);

    // synchronous, not wait for epoch
    status create_transaction_internal(transaction_handle& handle, transaction_option const& option);

};

inline api::impl::database& get_impl(api::database& db) {
    return unsafe_downcast<api::impl::database>(db);
}

} // namespace jogasaki::api::impl

