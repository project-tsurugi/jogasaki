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
#include <jogasaki/scheduler/task_scheduler.h>

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
    ) override {
        (void)schema;
        BOOST_ASSERT(table != nullptr);  //NOLINT
        std::string name{table->simple_name()};
        if (! kvs_db_) {
            LOG(ERROR) << "db not started";
            return status::err_invalid_state;
        }
        try {
            tables_->add_table(std::move(table));
        } catch(std::invalid_argument& e) {
            LOG(ERROR) << "table " << name << " already exists";
            return status::err_already_exists;
        }
        return status::ok;
    }

    std::shared_ptr<yugawara::storage::table const> do_find_table(
        std::string_view name,
        std::string_view schema
    ) override {
        (void)schema;
        if(auto res = tables_->find_table(name)) {
            return res;
        }
        return {};
    }

    status do_drop_table(
        std::string_view name,
        std::string_view schema
    ) override {
        (void)schema;
        if(tables_->remove_relation(name)) {
            return status::ok;
        }
        return status::not_found;
    }

    status do_create_index(
        std::shared_ptr<yugawara::storage::index> index,
        std::string_view schema
    ) override {
        (void)schema;
        BOOST_ASSERT(index != nullptr);  //NOLINT
        std::string name{index->simple_name()};
        if (! kvs_db_) {
            LOG(ERROR) << "db not started";
            return status::err_invalid_state;
        }
        try {
            tables_->add_index(std::move(index));
        } catch(std::invalid_argument& e) {
            LOG(ERROR) << "index " << name << " already exists";
            return status::err_already_exists;
        }
        kvs_db_->create_storage(name);
        return status::ok;
    }

    std::shared_ptr<yugawara::storage::index const> do_find_index(
        std::string_view name,
        std::string_view schema
    ) override {
        (void)schema;
        if(auto res = tables_->find_index(name)) {
            return res;
        }
        return {};
    }

    status do_drop_index(
        std::string_view name,
        std::string_view schema
    ) override {
        (void)schema;
        if(tables_->remove_index(name)) {
            // try to delete stroage on kvs.
            auto stg = kvs_db_->get_storage(name);
            if (! stg) {
                LOG(INFO) << "kvs storage " << name << " not found.";
                return status::ok;
            }
            if(auto res = stg->delete_storage(); res != status::ok) {
                LOG(ERROR) << res << " error on deleting storage " << name;
            }
            return status::ok;
        }
        return status::not_found;
    }

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
    std::unique_ptr<scheduler::task_scheduler> task_scheduler_{};
};

}

