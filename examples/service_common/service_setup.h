/*
 * Copyright 2018-2026 Project Tsurugi.
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
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/service.h>
#include <jogasaki/configuration.h>

#include "temporary_folder.h"

namespace jogasaki::common_cli {

/**
 * @brief override default values of the common flags
 * @details call this before gflags::ParseCommandLineFlags() so that each tool can have its own defaults
 * @param defaults pairs of flag name and default value
 */
void set_flag_defaults(std::vector<std::pair<std::string, std::string>> const& defaults);

/**
 * @brief fill the configuration from the common flags (scheduler, core assignment, database location)
 * @return false if the flags are inconsistent
 */
bool fill_configuration(configuration& cfg);

/**
 * @brief database and sql service running in-process
 */
class service_environment {
public:
    service_environment() = default;
    ~service_environment();
    service_environment(service_environment const&) = delete;
    service_environment& operator=(service_environment const&) = delete;
    service_environment(service_environment&&) = delete;
    service_environment& operator=(service_environment&&) = delete;

    /**
     * @brief create and start the database and sql service
     * @details the database location is taken from the flags. If it's "TMP", temporary directory is created.
     * @return false if the database failed to start
     */
    bool start(std::shared_ptr<configuration> cfg);

    /**
     * @brief stop the database and clean up the temporary directory
     */
    void stop();

    [[nodiscard]] api::database& database() noexcept {
        return *db_;
    }

    [[nodiscard]] std::shared_ptr<api::impl::service> const& service() const noexcept {
        return service_;
    }

private:
    std::shared_ptr<api::database> db_{};
    std::shared_ptr<api::impl::service> service_{};
    temporary_folder temporary_{};
    bool started_{};
};

/**
 * @brief register built-in tables if requested by the flags, and load/populate data
 * @details built-in tables are test tables (T0, T1, T2, ...) and benchmark tables (TPC-C like).
 * `--load_from` and `--prepare_data` are effective only if built-in tables are registered.
 * @return false on error
 */
bool setup_builtin_tables(api::database& db);

/**
 * @brief modulo used to generate values for `--prepare_data`
 * @details key columns of the populated records have values in [0, prepared_data_modulo)
 */
constexpr std::size_t prepared_data_modulo = 100;

}  // namespace jogasaki::common_cli
