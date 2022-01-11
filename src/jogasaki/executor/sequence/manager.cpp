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

#include <takatori/util/fail.h>
#include <yugawara/storage/configurable_provider.h>

#include <jogasaki/logging.h>
#include <jogasaki/constants.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/kvs/readable_stream.h>
#include <jogasaki/kvs/writable_stream.h>
#include <jogasaki/data/aligned_buffer.h>

#include <jogasaki/executor/sequence/sequence.h>
#include <jogasaki/executor/sequence/info.h>

namespace jogasaki::executor::sequence {

using takatori::util::fail;

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

std::size_t manager::load_id_map() {
    auto stg = db_->get_or_create_storage(system_sequences_name);
    auto tx = db_->create_transaction();
    std::unique_ptr<kvs::iterator> it{};
    if(auto res = stg->scan(
            *tx,
            "",
            kvs::end_point_kind::unbound,
            "",
            kvs::end_point_kind::unbound,
            it
        );
        res != status::ok || !it) {
        fail();
    }
    std::size_t ret{};
    while(status::ok == it->next()) {
        auto [def_id, seq_id] = read_entry(it);
        sequences_[def_id] = details::sequence_element(seq_id);
        ++ret;
    }
    if(tx->commit() != status::ok) {
        fail();
    }
    VLOG(log_debug) << "Sequences loaded from system table : " << ret;
    return ret;
}

sequence* manager::register_sequence(
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
        save_id_map();
    }
    return sequences_[def_id].sequence();
}

void manager::register_sequences(
    maybe_shared_ptr<yugawara::storage::configurable_provider> const& provider
) {
    provider->each_sequence(
        [this](
            std::string_view,
            std::shared_ptr<yugawara::storage::sequence const> const& entry
        ) {
            auto def_id = *entry->definition_id();
            register_sequence(
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
    save_id_map();
}

sequence* manager::find_sequence(sequence_definition_id def_id) {
    if (sequences_.count(def_id) == 0) {
        return nullptr;
    }
    return sequences_[def_id].sequence();
}

bool manager::notify_updates(kvs::transaction& tx) {
    if (used_sequences_.count(std::addressof(tx)) == 0) {
        return true;
    }
    for(auto* p : used_sequences_[std::addressof(tx)]) {
        auto s = p->get();
        if (!db_->update_sequence(tx, p->info().id(), s.version_, s.value_)) {
            return false;
        }
    }
    used_sequences_[std::addressof(tx)].clear();
    return true;
}

bool manager::remove_sequence(sequence_definition_id def_id) {
    if (sequences_.count(def_id) == 0) {
        return false;
    }
    if (auto res = db_->delete_sequence(sequences_[def_id].id()); !res) {
        fail();
    }
    remove_id_map(def_id);
    sequences_.erase(def_id);
    return true;
}

void manager::mark_sequence_used_by(kvs::transaction& tx, sequence& seq) {
    used_sequences_[std::addressof(tx)].emplace(std::addressof(seq));
}

using kind = meta::field_type_kind;

std::pair<sequence_definition_id, sequence_id> manager::read_entry(std::unique_ptr<kvs::iterator>& it) {
    std::string_view k{};
    std::string_view v{};
    if (!it->key(k) || !it->value(v)) {
        fail();
    }
    kvs::readable_stream key{k.data(), k.size()};
    kvs::readable_stream value{v.data(), v.size()};
    executor::process::impl::expression::any dest{};
    kvs::decode(key, meta::field_type{meta::field_enum_tag<kind::int8>}, kvs::spec_key_ascending, dest);
    sequence_definition_id def_id{};
    sequence_id id{};
    def_id = dest.to<std::int64_t>();
    kvs::decode_nullable(value, meta::field_type{meta::field_enum_tag<kind::int8>}, kvs::spec_value, dest);
    id = dest.to<std::int64_t>();
    return {def_id, id};
}

void manager::save_id_map() {
    auto stg = db_->get_or_create_storage(system_sequences_name);
    auto tx = db_->create_transaction();
    data::aligned_buffer key_buf{10};
    data::aligned_buffer val_buf{10};
    for(auto& [def_id, element] : sequences_) {
        auto id = element.id();
        kvs::writable_stream key{key_buf.data(), key_buf.size()};
        kvs::writable_stream value{val_buf.data(), val_buf.size()};
        executor::process::impl::expression::any k{std::in_place_type<std::int64_t>, def_id};
        executor::process::impl::expression::any v{std::in_place_type<std::int64_t>, id};
        kvs::encode(k, meta::field_type{meta::field_enum_tag<kind::int8>}, kvs::spec_key_ascending, key);
        kvs::encode_nullable(v, meta::field_type{meta::field_enum_tag<kind::int8>}, kvs::spec_value, value);
        if (auto res = stg->put(
                *tx,
                {key.data(), key.size()},
                {value.data(), value.size()}
            ); res != status::ok) {
            fail();
        }
    }
    if(status::ok != tx->commit()) {
        fail();
    }
}

manager::sequences_type const& manager::sequences() const noexcept {
    return sequences_;
}

void manager::remove_id_map(sequence_definition_id def_id) {
    auto stg = db_->get_or_create_storage(system_sequences_name);
    auto tx = db_->create_transaction();

    data::aligned_buffer key_buf{10};
    kvs::writable_stream key{key_buf.data(), key_buf.size()};
    executor::process::impl::expression::any k{std::in_place_type<std::int64_t>, def_id};
    kvs::encode(k, meta::field_type{meta::field_enum_tag<kind::int8>}, kvs::spec_key_ascending, key);
    if (auto res = stg->remove(
            *tx,
            {key.data(), key.size()}
        ); res != status::ok && res != status::not_found) {
        fail();
    }
    if(tx->commit() != status::ok) {
        fail();
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
