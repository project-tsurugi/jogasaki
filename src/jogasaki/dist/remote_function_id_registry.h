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
#include <cstdint>
#include <memory>
#include <mutex>
#include <set>
#include <vector>

#include <jogasaki/status.h>

namespace jogasaki::dist {

class remote_function_id_registry;

/**
 * @brief coordinator-side definition IDs reserved for the function declarations of one remote catalog.
 * @details The IDs are released when the lease is destroyed. remote_catalog_manager makes every restored
 * declaration share the lease, so the IDs stay reserved until the last reference to those declarations is gone.
 */
class remote_function_id_lease {
public:
    using definition_id_type = std::uint64_t;

    remote_function_id_lease(
        std::weak_ptr<remote_function_id_registry> owner,
        std::vector<definition_id_type> functions,
        std::vector<definition_id_type> aggregate_functions) noexcept;

    ~remote_function_id_lease();
    remote_function_id_lease(remote_function_id_lease const&) = delete;
    remote_function_id_lease& operator=(remote_function_id_lease const&) = delete;
    remote_function_id_lease(remote_function_id_lease&&) = delete;
    remote_function_id_lease& operator=(remote_function_id_lease&&) = delete;

    [[nodiscard]] std::vector<definition_id_type> const& functions() const noexcept;
    [[nodiscard]] std::vector<definition_id_type> const& aggregate_functions() const noexcept;

private:
    std::weak_ptr<remote_function_id_registry> owner_{};
    std::vector<definition_id_type> functions_{};
    std::vector<definition_id_type> aggregate_functions_{};
};

/**
 * @brief allocation state of coordinator-side definition IDs for remote function declarations.
 * @details Owned by the database, not by remote_catalog_manager. Regular and aggregate functions use separate
 * ID spaces. IDs start at minimum_id, never use 0, skip IDs excluded for local declarations,
 * and are reused only after their lease is released. Local declarations must be excluded before allocation.
 */
class remote_function_id_registry : public std::enable_shared_from_this<remote_function_id_registry> {
public:
    using definition_id_type = remote_function_id_lease::definition_id_type;

    /// @brief provisional first ID of remote catalog declarations (Jogasaki implementation choice)
    static constexpr definition_id_type default_minimum_id = definition_id_type{1} << 48U;

    explicit remote_function_id_registry(definition_id_type minimum_id = default_minimum_id) noexcept;

    /**
     * @brief marks an ID used by a local (built-in or user-defined) regular function declaration.
     */
    void exclude_function(definition_id_type id);

    /**
     * @brief marks an ID used by a local aggregate function declaration.
     */
    void exclude_aggregate_function(definition_id_type id);

    /**
     * @brief reserves IDs for all function declarations of one catalog at once.
     * @return status::ok, or status::err_resource_limit_reached if an ID space is exhausted; nothing is reserved then
     */
    [[nodiscard]] status reserve(
        std::size_t functions,
        std::size_t aggregate_functions,
        std::shared_ptr<remote_function_id_lease>& out);

    [[nodiscard]] bool function_reserved(definition_id_type id) const;
    [[nodiscard]] bool aggregate_function_reserved(definition_id_type id) const;
    [[nodiscard]] std::size_t reserved_count() const;

private:
    friend class remote_function_id_lease;

    struct id_space {
        std::set<definition_id_type> excluded{};
        std::set<definition_id_type> reserved{};
    };

    definition_id_type minimum_id_{};
    mutable std::mutex mutex_{};
    id_space functions_{};
    id_space aggregate_functions_{};

    [[nodiscard]] bool allocate(id_space& space, std::size_t count, std::vector<definition_id_type>& out) const;
    void release(
        std::vector<definition_id_type> const& functions,
        std::vector<definition_id_type> const& aggregate_functions) noexcept;
};

}  // namespace jogasaki::dist
