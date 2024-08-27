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

#include <atomic>
#include <cstddef>
#include <limits>
#include <memory>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <tbb/concurrent_hash_map.h>

#include <takatori/util/maybe_shared_ptr.h>
#include <yugawara/storage/configurable_provider.h>

#include <jogasaki/common_types.h>
#include <jogasaki/executor/sequence/info.h>
#include <jogasaki/executor/sequence/sequence.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/iterator.h>
#include <jogasaki/kvs/transaction.h>

namespace jogasaki::executor::sequence {

using takatori::util::maybe_shared_ptr;

class sequence;

namespace details {

class sequence_element {
public:
    constexpr static sequence_id undefined_id = static_cast<sequence_id>(-1);

    sequence_element() = default;

    explicit sequence_element(sequence_id id);

    [[nodiscard]] sequence_id id() const noexcept;
    std::unique_ptr<class info> const& info(std::unique_ptr<class info> info) noexcept;
    [[nodiscard]] class info* info() const noexcept;

    std::unique_ptr<class sequence> const& sequence(std::unique_ptr<class sequence> sequence) noexcept;

    [[nodiscard]] class sequence* sequence() const noexcept;

private:
    sequence_id sequence_id_{undefined_id};
    std::unique_ptr<class info> info_{};
    std::unique_ptr<class sequence> sequence_{};
};

}

/**
 * @brief sequence manager
 * @details this object owns in-memory sequence objects, provides APIs to get current/next sequence values,
 * and manages sequence objects synchronization with kvs layer.
 */
class manager {
    friend class sequence;

public:
    /**
     * @brief mapping between sequence definition id and sequence id
     * @details manager maintains this mapping so that the definition id (owned by SQL engine) can be
     * resolved to sequence id (defined by tx engine).
     */
    using id_map_type = std::unordered_map<sequence_definition_id, sequence_id>;

    /**
     * @brief sequence entities type
     */
    using sequences_type = std::unordered_map<sequence_definition_id, details::sequence_element>;

    /**
     * @brief create empty object
     */
    manager() = default;

    /**
     * @brief destruct object
     */
    ~manager() = default;

    manager(manager const& other) = default;
    manager& operator=(manager const& other) = default;
    manager(manager&& other) noexcept = default;
    manager& operator=(manager&& other) noexcept = default;

    /**
     * @brief create new manager object
     * @param db database where the sequences are stored/saved
     * @param id_map definition id to sequence id map
     */
    explicit manager(kvs::database& db, id_map_type const& id_map = {}) noexcept;

    /**
     * @brief load sequence id mapping from system_sequences table and initialize in-memory sequence objects.
     * This function is not thread-safe. Only a thread can call this member function at a time.
     * @returns the number of sequence entries read from the system table
     * @throws sequence::exception if any error occurs, then passed transaction is aborted.
     */
    std::size_t load_id_map(kvs::transaction* tx = nullptr);

    /**
     * @brief register the sequence properties for the definition id
     * @details using the id_map currently held, create the in-memory sequence object with the given spec.
     * If the id_map doesn't has the given def_id, ask kvs to assign new sequence id. Optionally save the id map.
     * This function is not thread-safe. Only a thread can call this member function at a time.
     * @param def_id the key to uniquely identify the sequence
     * @param name the name of the sequence
     * @param initial_value initial value of the sequence
     * @param increment the increment value of the sequence
     * @param minimum_value minimum value allowed for the sequence
     * @param maximum_value maximum value allowed for the sequence
     * @param enable_cycle whether cycle is enabled or not. If enabled, out-of-range assignment results in
     * min or max value (corresponding to the boundary that is went over.)
     * @param save_id_map_entry indicates whether the id_map entry for registered sequence should be saved now.
     * Set false if you are registering multiples sequences and they should be saved later.
     * @return the in-memory sequence object just registered
     * @throws sequence::exception if any error occurs, then passed transaction is aborted.
     */
    sequence* register_sequence(
        kvs::transaction* tx,
        sequence_definition_id def_id,
        std::string_view name,
        sequence_value initial_value = 0,
        sequence_value increment = 1,
        sequence_value minimum_value = 0,
        sequence_value maximum_value = std::numeric_limits<sequence_value>::max(),
        bool enable_cycle = true,
        bool save_id_map_entry = true
    );

    /**
     * @brief bulk registering sequences
     * @details this function retrieves sequence definitions from provider and register one by one.
     * This function is not thread-safe. Only a thread can call this member function at a time.
     * @param provider the config. provider that gives sequences definitions.
     * @throws sequence::exception if any error occurs, then passed transaction is aborted.
     */
    void register_sequences(
        kvs::transaction* tx,
        maybe_shared_ptr<yugawara::storage::configurable_provider> const& provider
    );

    /**
     * @brief find sequence
     * This function can be called from multiple threads as far as it doesn't compete with functions modifying sequences
     * (i.e. load_id_map(), register_sequence(), register_sequences() and remove_sequence()).
     * @param def_id the sequence definition id for search
     * @return the sequence object if found
     * @return nullptr if not found
     */
    sequence* find_sequence(sequence_definition_id def_id) const;

    /**
     * @brief notifies kvs of the current sequence value so that they are made durable together with the updating tx
     * @details when sequence value is updated with sequence::next() call, this function must be called before commit
     * of the transaction that calls next(). Otherwise, the updates are not sent to kvs and results in losing updates
     * for the sequences.
     * This function is thread-safe and multiple threads can call simultaneously as long as passed tx are different.
     * @param tx the transaction that updated the sequence and the value needs to be durable together
     * @return true if successful
     * @return false otherwise
     * @throws sequence::exception if any error occurs, then passed transaction is aborted.
     */
    bool notify_updates(kvs::transaction& tx);

    /**
     * @brief remove the sequence (the in-memory sequence object, id_map entry and kvs object) completely
     * This function is not thread-safe. Only a thread can call this member function at a time.
     * @param def_id the definition id of the sequence to be removed
     * @return true if successful
     * @return false otherwise
     * @throws sequence::exception if any error occurs, then passed transaction is aborted.
     */
    bool remove_sequence(
        sequence_definition_id def_id,
        kvs::transaction* tx = nullptr
    );

    /**
     * @brief accessor to the in-memory sequences objects
     * @return the sequences held by this manager
     */
    sequences_type const& sequences() const noexcept;

private:
    kvs::database* db_{};
    sequences_type sequences_{};
    tbb::concurrent_hash_map<kvs::transaction*, std::unordered_set<sequence*>> used_sequences_{};

    /**
     * @brief mark the sequence used by given transaction
     * This function is thread-safe and multiple threads can call simultaneously.
     */
    void mark_sequence_used_by(kvs::transaction& tx, sequence& seq);

    void save_id_map(kvs::transaction* tx);
    void remove_id_map(sequence_definition_id def_id, kvs::transaction* tx);
};

}  // namespace jogasaki::executor::sequence
