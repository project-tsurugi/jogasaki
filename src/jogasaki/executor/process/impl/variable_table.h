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

#include <atomic>
#include <deque>
#include <iosfwd>
#include <memory>
#include <unordered_map>
#include <vector>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/data/small_record_store.h>
#include <jogasaki/error/error_info.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/lob/lob_id.h>
#include <jogasaki/lob/lob_reference.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/status.h>
#include <jogasaki/transaction_context.h>

namespace jogasaki::executor::process::impl {

using takatori::util::maybe_shared_ptr;

/**
 * @brief variables storage
 */
class variable_table {
public:
    using lob_locator = std::variant<lob::blob_locator, lob::clob_locator>;

    /**
     * @brief construct empty instance
     */
    variable_table() = default;

    /**
     * @brief construct new instance
     */
    explicit variable_table(
        variable_table_info const& info
    );

    /**
     * @brief accessor to variable store
     */
    [[nodiscard]] data::small_record_store& store() const noexcept;

    /**
     * @brief accessor to metadata of variable store
     */
    [[nodiscard]] maybe_shared_ptr<meta::record_meta> const& meta() const noexcept;

    /**
     * @brief accessor to variable table info
     */
    [[nodiscard]] variable_table_info const& info() const noexcept;

    /**
     * @brief return whether the object is non-empty
     */
    [[nodiscard]] explicit operator bool() const noexcept;

    /**
     * @brief Support for debugging, callable in GDB: vt->dump()
     *
     * @param indent A string used for indentation in the output,
     * making it easier to read nested structures. Default is an empty string.
     */
    void dump(std::string const& indent = "") const noexcept;

    /**
     * @brief add lob locator to maintain the locator lifecycle for blob reference
     */
    template <class T>
    std::enable_if_t<std::is_same_v<lob::blob_locator, T> || std::is_same_v<lob::clob_locator, T>, T> const& add_lob_locator(T locator) {
        auto& entry = lob_entries_.emplace_back(lob_locator{std::in_place_type<T>, std::forward<T>(locator)});
        auto& ret = std::get<T>(entry.locator_);
        lob_entries_index_.emplace(static_cast<lob::lob_locator const*>(std::addressof(ret)), std::addressof(entry));
        return ret;
    }

    /**
     * @brief register the temporary lob files held by this table to datastore
     * @details the temporary lob file is moved to datastore on registration, so it must be registered only once
     * even if the lob is referenced multiple times. This function must be called before the statement execution
     * starts evaluating the host variables in multiple threads.
     * @param tx transaction to keep the scope object (blob pool) for the lob data
     * @param error [out] error information is set when status code other than status::ok is returned
     * @return status::ok when successful
     * @return any other error otherwise
     * @note this function is not thread-safe
     */
    status register_temporary_lobs(
        transaction_context* tx,
        std::shared_ptr<error::error_info>& error
    );

    /**
     * @brief assign lob id for the `provided` lob reference held by this table
     * @details if the lob is registered by register_temporary_lobs(), the first call takes the registered lob id and
     * the subsequent calls duplicate it. Otherwise, the lob file is registered (i.e. copied) to datastore on each call.
     * @param ref the `provided` lob reference whose locator is held by this table
     * @param tx transaction to keep the scope object (blob pool) for the lob data
     * @param id [out] lob id assigned for the lob data, unique for each call
     * @param error [out] error information is set when status code other than status::ok is returned
     * @return status::ok when successful
     * @return any other error otherwise
     * @note this function is thread-safe and lock-free
     */
    status assign_lob_id(
        lob::lob_reference const& ref,
        transaction_context* tx,
        lob::lob_id_type& id,
        std::shared_ptr<error::error_info>& error
    ) const;

private:
    /**
     * @brief lob locator and its registration state
     */
    struct lob_entry {
        explicit lob_entry(lob_locator locator) : locator_(std::move(locator)) {}

        lob_locator locator_;
        // the lob id registered by register_temporary_lobs()
        lob::lob_id_type id_{};
        bool registered_{};
        // whether the registered lob id is already taken by assign_lob_id()
        mutable std::atomic_bool taken_{false};
    };

    variable_table_info const* info_{};
    std::unique_ptr<data::small_record_store> store_{};
    std::deque<lob_entry> lob_entries_{};
    // not modified during statement execution, so that it can be read without lock
    std::unordered_map<lob::lob_locator const*, lob_entry*> lob_entries_index_{};
};

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
std::ostream& operator<<(std::ostream& out, variable_table const& value);

/**
 * @brief list of variable tables, one per basic block in the process
 */
using variable_table_list = std::vector<variable_table>;

}  // namespace jogasaki::executor::process::impl
