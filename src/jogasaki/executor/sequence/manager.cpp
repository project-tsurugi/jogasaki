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
#include "manager.h"

#include <atomic>
#include <unordered_set>

#include <takatori/util/exception.h>
#include <yugawara/storage/configurable_provider.h>

#include <jogasaki/logging.h>
#include <jogasaki/constants.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/kvs/readable_stream.h>
#include <jogasaki/kvs/writable_stream.h>
#include <jogasaki/data/aligned_buffer.h>
#include <jogasaki/data/any.h>

#include <jogasaki/executor/sequence/sequence.h>
#include <jogasaki/executor/sequence/info.h>

#include "metadata_store.h"

namespace jogasaki::executor::sequence {

using takatori::util::throw_exception;
using kind = meta::field_type_kind;

manager::sequences_type create_sequences(manager::id_map_type const& id_map) {
    manager::sequences_type ret{};
    for(auto& [def_id, id] : id_map) {
        auto [it, success] = ret.try_emplace(def_id, id);
        BOOST_ASSERT(success);  //NOLINT
        (void)success;
        (void)it;
    }
    return ret;
}

manager::manager(
    kvs::database& db,
    id_map_type const& id_map
) noexcept :
    db_(std::addressof(db)),
    sequences_(create_sequences(id_map))
{}

std::size_t manager::load_id_map(kvs::transaction* tx) {
    std::unique_ptr<kvs::transaction> created_tx{};
    if (! tx) {
        // for testing purpose
        created_tx = db_->create_transaction();
        tx = created_tx.get();
    }
    metadata_store s{*tx};

    std::size_t ret{};
    if(auto res = s.scan([this, &ret](std::int64_t def_id, std::int64_t id) {
            sequences_[def_id] = details::sequence_element(id);
            ++ret;
        }); ! res) {
        std::stringstream ss{};
        ss << "Sequences scan failed : " << res;
        throw_exception(std::logic_error{ss.str()});
    }
    if (created_tx) {
        (void)created_tx ->commit();
    }
    VLOG(log_debug) << "Sequences loaded from system table : " << ret;
    return ret;
}

sequence* manager::register_sequence(
    kvs::transaction* tx,
    sequence_definition_id def_id,
    std::string_view name,
    sequence_value initial_value,
    sequence_value increment,
    sequence_value minimum_value,
    sequence_value maximum_value,
    bool enable_cycle,
    bool save_id_map_entry
) {
    sequence_id seq_id{};
    if(sequences_.count(def_id) == 0) {
        seq_id = db_->create_sequence();
        sequences_[def_id] = details::sequence_element{seq_id};
    } else {
        seq_id = sequences_[def_id].id();
    }
    auto& p = sequences_[def_id].info(
        std::make_unique<info>(
            def_id,
            seq_id,
            name,
            initial_value,
            increment,
            minimum_value,
            maximum_value,
            enable_cycle
        )
    );

    auto [version, value] = db_->read_sequence(seq_id);
    if (version == version_invalid || version == 0) {
        version = 1;
        value = initial_value;
    }
    sequences_[def_id].sequence(std::make_unique<sequence>(*p, *this, version, value));

    if (save_id_map_entry) {
        save_id_map(tx);
    }
    return sequences_[def_id].sequence();
}

void manager::register_sequences(
    kvs::transaction* tx,
    maybe_shared_ptr<yugawara::storage::configurable_provider> const& provider
) {
    provider->each_sequence(
        [this, &tx](
            std::string_view,
            std::shared_ptr<yugawara::storage::sequence const> const& entry
        ) {
            auto def_id = *entry->definition_id();
            register_sequence(
                tx,
                def_id,
                entry->simple_name(),
                entry->initial_value(),
                entry->increment_value(),
                entry->min_value(),
                entry->max_value(),
                entry->cycle(),
                false
            );
        }
    );
    save_id_map(tx);
}

sequence* manager::find_sequence(sequence_definition_id def_id) const {
    if (sequences_.count(def_id) == 0) {
        return nullptr;
    }
    return sequences_.at(def_id).sequence();
}

bool manager::notify_updates(kvs::transaction& tx) {
    std::unordered_set<sequence*> copy{};
    {
        decltype(used_sequences_)::accessor acc{};
        if (!used_sequences_.find(acc, std::addressof(tx))) {
            return true;
        }
        copy = acc->second;
        used_sequences_.erase(acc);
    }
    for(auto* p : copy) {
        auto s = p->get();
        if (!db_->update_sequence(tx, p->info().id(), s.version_, s.value_)) {
            return false;
        }
    }
    return true;
}

bool manager::remove_sequence(
    sequence_definition_id def_id,
    kvs::transaction* tx
) {
    if (sequences_.count(def_id) == 0) {
        return false;
    }
    if (auto res = db_->delete_sequence(sequences_[def_id].id()); !res) {
        throw_exception(std::logic_error{""});
    }
    remove_id_map(def_id, tx);
    sequences_.erase(def_id);
    return true;
}

void manager::mark_sequence_used_by(kvs::transaction& tx, sequence& seq) {
    decltype(used_sequences_)::accessor acc{};
    used_sequences_.insert(acc, std::addressof(tx));
    acc->second.emplace(std::addressof(seq));
}


void manager::save_id_map(kvs::transaction* tx) {
    std::unique_ptr<kvs::transaction> created_tx{};
    if (! tx) {
        // for testing purpose
        created_tx = db_->create_transaction();
        tx = created_tx.get();
    }
    metadata_store s{*tx};
    for(auto& [def_id, element] : sequences_) {
        auto id = element.id();
        if(auto res = s.put(def_id, id); ! res) {
            throw_exception(std::logic_error{""});
        }
    }
    if (created_tx) {
        (void) created_tx->commit();
    }
}

manager::sequences_type const& manager::sequences() const noexcept {
    return sequences_;
}

void manager::remove_id_map(
    sequence_definition_id def_id,
    kvs::transaction* tx
) {
    std::unique_ptr<kvs::transaction> created_tx{};
    if (! tx) {
        // for testing purpose
        created_tx = db_->create_transaction();
        tx = created_tx.get();
    }
    metadata_store s{*tx};
    if(! s.remove(def_id)) {
        throw_exception(std::logic_error{""});
    }
    if (created_tx) {
        (void) created_tx->commit();
    }
}

details::sequence_element::sequence_element(sequence_id id) : sequence_id_(id) {}

sequence_id details::sequence_element::id() const noexcept {
    return sequence_id_;
}

std::unique_ptr<class info> const& details::sequence_element::info(std::unique_ptr<class info> info) noexcept {
    info_ = std::move(info);
    return info_;
}

class info* details::sequence_element::info() const noexcept {
    return info_.get();
}

std::unique_ptr<class sequence> const&
details::sequence_element::sequence(std::unique_ptr<class sequence> sequence) noexcept {
    sequence_ = std::move(sequence);
    return sequence_;
}

class sequence* details::sequence_element::sequence() const noexcept {
    return sequence_.get();
}

}
