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

#include <takatori/type/data.h>

#include <jogasaki/accessor/const_record_ref.h>
#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/status.h>

namespace jogasaki {
class request_context;
namespace memory { class lifo_paged_memory_resource; }
namespace meta { class record_meta; }
}

namespace jogasaki::value_operations {

/** @brief value and nullity locations in a record; both offsets must be valid */
struct record_position {
    std::size_t value_offset{};
    std::size_t nullity_offset{};
};

/** @brief physical destination schema; it need not be a projection of its declared type */
struct record_destination {
    meta::field_type storage_type;
    record_position position;

    /** @brief bind physical type and both offsets from one schema column */
    [[nodiscard]] static record_destination from_record(meta::record_meta const& schema, std::size_t index);
};

class prepared_record_assignment;
class prepared_record_unification;

namespace details {
/** @brief shared implementation of borrowed record-to-record transfers */
class record_transfer {
    friend class ::jogasaki::value_operations::prepared_record_assignment;
    friend class ::jogasaki::value_operations::prepared_record_unification;

    record_transfer(takatori::type::data const& source_type, takatori::type::data const& target_type,
        meta::field_type source_storage, record_position source, record_destination target);

    template<class Convert>
    status apply_borrowed(accessor::const_record_ref source, accessor::record_ref target, Convert const& convert) const;

    takatori::type::data const* source_type_;
    takatori::type::data const* target_type_;
    meta::field_type source_storage_;
    record_position source_;
    record_destination target_;
    bool requires_conversion_;
};
} // namespace details

/**
 * @brief prepared SQL assignment from a nullable source field to a destination field
 * @details Preparation binds types, positions, conversion need and the NULL policy once.
 * Declared types are borrowed and must outlive this plan and its copies. No clone is performed.
 */
class prepared_record_assignment {
public:
    [[nodiscard]] static prepared_record_assignment prepare(
        takatori::type::data const& source_type, takatori::type::data const& target_type,
        record_position source, record_position target, bool target_nullable);

    // Preparation borrows types; temporaries must not escape into a prepared plan.
    static prepared_record_assignment prepare(takatori::type::data const&&, takatori::type::data const&,
        record_position, record_position, bool) = delete;
    static prepared_record_assignment prepare(takatori::type::data const&, takatori::type::data const&&,
        record_position, record_position, bool) = delete;

    /**
     * @brief check source NULL, apply assignment conversion if needed, and store the result
     * @note output variable-length data borrows source or conversion storage. Both must remain
     * alive until the output is consumed/copied. This operation does not rewind storage.
     * SQL conversion and constraint errors retain the existing request_context contract.
     */
    [[nodiscard]] status apply_borrowed(accessor::const_record_ref source, accessor::record_ref target,
        request_context& context, memory::lifo_paged_memory_resource* storage) const;

private:
    prepared_record_assignment(details::record_transfer transfer, bool target_nullable);
    details::record_transfer transfer_;
    bool target_nullable_;
};

/**
 * @brief prepared unifying conversion for exchange output; NULL is propagated
 * @details Declared types are borrowed and must outlive this plan and its copies.
 * The physical destination type is supplied from the exchange schema, never recomputed.
 * Caller-specific error remapping remains outside this operation.
 */
class prepared_record_unification {
public:
    [[nodiscard]] static prepared_record_unification prepare(
        takatori::type::data const& source_type, takatori::type::data const& target_type,
        record_position source, record_destination target);

    static prepared_record_unification prepare(takatori::type::data const&&, takatori::type::data const&,
        record_position, record_destination) = delete;
    static prepared_record_unification prepare(takatori::type::data const&, takatori::type::data const&&,
        record_position, record_destination) = delete;

    /** @brief convert and store, with the same borrowed-output lifetime as SQL assignment */
    [[nodiscard]] status apply_borrowed(accessor::const_record_ref source, accessor::record_ref target,
        memory::lifo_paged_memory_resource* storage) const;

private:
    explicit prepared_record_unification(details::record_transfer transfer);
    details::record_transfer transfer_;
};

} // namespace jogasaki::value_operations
