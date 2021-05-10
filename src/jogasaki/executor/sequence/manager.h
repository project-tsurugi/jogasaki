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

#include <atomic>
#include <unordered_set>

#include <takatori/util/maybe_shared_ptr.h>
#include <yugawara/storage/configurable_provider.h>

#include <jogasaki/common_types.h>
#include <jogasaki/kvs/transaction.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/iterator.h>
#include <jogasaki/executor/sequence/info.h>

namespace jogasaki::executor::sequence {

using takatori::util::maybe_shared_ptr;

class sequence;

namespace details {
class sequence_element {
public:
    constexpr static sequence_id undefined_id = static_cast<sequence_id>(-1);

    sequence_element() = default;

    explicit sequence_element(sequence_id id) : sequence_id_(id) {}

    [[nodiscard]] sequence_id id() const noexcept {
        return sequence_id_;
    }
    std::unique_ptr<class info> const& info(std::unique_ptr<class info> info) noexcept {
        info_ = std::move(info);
        return info_;
    }
    [[nodiscard]] class info* info() const noexcept {
        return info_.get();
    }

    std::unique_ptr<class sequence> const& sequence(std::unique_ptr<class sequence> sequence) noexcept {
        sequence_ = std::move(sequence);
        return sequence_;
    }

    [[nodiscard]] class sequence* sequence() const noexcept {
        return sequence_.get();
    }

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
 * This object is thread-safe and multiple threads/transactions can use this object simultaneously.
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
     * @returns the number of sequence entries read from the system table
     */
    std::size_t load_id_map();

    /**
     * @brief register the sequence properties for the definition id
     * @details using the id_map currently held, create the in-memory sequence object with the given spec.
     * If the id_map doesn't has the given def_id, ask kvs to assign new sequence id. Optionally save the id map.
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
     */
    sequence* register_sequence(
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
     * @param provider the config. provider that gives sequences definitions.
     */
    void register_sequences(maybe_shared_ptr<yugawara::storage::configurable_provider> const& provider);

    /**
     * @brief find sequence
     * @param def_id the sequence definition id for search
     * @return the sequence object if found
     * @return nullptr if not found
     */
    sequence* find_sequence(sequence_definition_id def_id);

    /**
     * @brief notifies kvs of the current sequence value so that they are made durable together with the updating tx
     * @details when sequence value is updated with sequence::next() call, this function must be called before commit
     * of the transaction that calls next(). Otherwise, the updates are not sent to kvs and results in losing updates
     * for the sequences.
     * @param tx the transaction that updated the sequence and the value needs to be durable together
     * @return true if successful
     * @return false otherwise
     */
    bool notify_updates(kvs::transaction& tx);

    /**
     * @brief remove the sequence (the in-memory sequence object, id_map entry and kvs object) completely
     * @param def_id the definition id of the sequence to be removed
     * @return true if successful
     * @return false otherwise
     */
    bool remove_sequence(sequence_definition_id def_id);

    /**
     * @brief accessor to the in-memory sequences objects
     * @return the sequences held by this manager
     */
    sequences_type const& sequences() const noexcept;

private:
    kvs::database* db_{};
    sequences_type sequences_{};
    std::unordered_map<kvs::transaction*, std::unordered_set<sequence*>> used_sequences_{};

    void mark_sequence_used_by(kvs::transaction& tx, sequence& seq);
    std::pair<sequence_definition_id, sequence_id> read_entry(std::unique_ptr<kvs::iterator>& it);
    void save_id_map();
    void remove_id_map(sequence_definition_id def_id);
};

}
