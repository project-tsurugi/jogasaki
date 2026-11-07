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

#include <memory>
#include <unordered_map>
#include <utility>

#include <takatori/plan/process.h>

#include <jogasaki/dist/operation_location.h>
#include <jogasaki/dist/remote_catalog_provider.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/meta/external_record_meta.h>
#include <jogasaki/plan/statement_work_level.h>
#include <jogasaki/storage/storage_list.h>
#include <jogasaki/storage/storage_operation.h>

namespace jogasaki::plan {

/**
 * @brief mirror objects container
 */
class mirror_container {
public:
    using step_index = takatori::plan::process const*;

    using variable_definition = std::pair<
        std::shared_ptr<executor::process::impl::variables_info_list>,
        std::shared_ptr<executor::process::impl::block_indices>
    >;

    /**
     * @brief create new object
     */
    mirror_container() = default;

    /**
     * @brief destruct the object
     */
    ~mirror_container() = default;

    mirror_container(mirror_container const& other) = delete;
    mirror_container& operator=(mirror_container const& other) = delete;
    mirror_container(mirror_container&& other) noexcept = default;
    mirror_container& operator=(mirror_container&& other) noexcept = default;

    /**
     * @brief accessor to variable definition
     * @param index the identifier for the step
     * @return reference to the variable definition
     */
    [[nodiscard]] variable_definition const& at(step_index index) const noexcept;

    /**
     * @brief set the variable definition for the specific step
     * @param index the identifier for the step
     * @param def the variable definition
     */
    void set(step_index index, variable_definition def) noexcept;

    /**
     * @brief host variable info setter
     * @param host_variable_info the host variable information to be stored in this container
     */
    void host_variable_info(std::shared_ptr<executor::process::impl::variable_table_info> host_variable_info) noexcept;

    /**
     * @brief accessor to the host variable information
     */
    std::shared_ptr<executor::process::impl::variable_table_info> const& host_variable_info() const noexcept;

    /**
     * @brief set the meta data information for the external writer used by the Emit operator (if any)
     * @param meta the record metadata for the external write
     */
    void external_writer_meta(std::shared_ptr<meta::external_record_meta> meta) noexcept;

    /**
     * @brief accessor to the external writer meta data
     * @returns external writer metadata
     */
    std::shared_ptr<meta::external_record_meta> const& external_writer_meta() const noexcept;

    /**
     * @brief accessor to the statement work level
     * @returns statement work level
     */
    [[nodiscard]] statement_work_level& work_level() noexcept;

    /**
     * @brief non-const accessor for the storage operation object
     * @note use this only when constructing the prepared statement
     */
    storage::storage_operation& mutable_storage_operation() noexcept;

    /**
     * @brief const accessor for the storage operation object
     */
    [[nodiscard]] storage::storage_operation const& storage_operation() const noexcept;

    /**
     * @brief set the process placement captured from the compiler's operation location result
     * @param locations the captured placement, or nullptr if the compiler provided no such result
     */
    void process_locations(std::shared_ptr<dist::process_locations const> locations) noexcept;

    /**
     * @brief accessor to the captured process placement
     * @returns the placement, or nullptr if not captured
     */
    [[nodiscard]] std::shared_ptr<dist::process_locations const> const& process_locations() const noexcept;

    // Prepared and executable statements share this snapshot after catalog DROP.
    void remote_catalogs(std::shared_ptr<dist::remote_catalog_snapshot const> snapshot) noexcept {
        remote_catalogs_ = std::move(snapshot);
    }
    [[nodiscard]] std::shared_ptr<dist::remote_catalog_snapshot const> const& remote_catalogs() const noexcept {
        return remote_catalogs_;
    }

private:
    std::unordered_map<step_index, variable_definition> variable_definitions_{};
    std::shared_ptr<executor::process::impl::variable_table_info> host_variable_info_{};
    std::shared_ptr<meta::external_record_meta> external_writer_meta_{};
    statement_work_level work_level_{};
    storage::storage_operation storage_operation_{};
    std::shared_ptr<dist::process_locations const> process_locations_{};
    std::shared_ptr<dist::remote_catalog_snapshot const> remote_catalogs_{};
};

} // namespace jogasaki::plan
