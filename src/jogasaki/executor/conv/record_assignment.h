/*
 * Copyright 2026 Project Tsurugi.
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
#include <optional>
#include <takatori/type/data.h>
#include <jogasaki/data/field_operations.h>
#include <jogasaki/status.h>
namespace jogasaki {
class request_context;
namespace memory { class lifo_paged_memory_resource; }
}
namespace jogasaki::executor::conv {
enum class conversion_kind { none, assignment, unify };
struct field_position { std::size_t value; std::size_t nullity; };
/** Prepared conversion and storage. SQL types must outlive the plan. Varlen storage belongs to the caller.
 * Context is optional for copies/unification; SQL assignment conversion requires it.
 */
class record_assignment {
public:
    [[nodiscard]] static record_assignment prepare_value(takatori::type::data const& source,
        takatori::type::data const& target, meta::field_type const& storage, field_position destination,
        bool nullable, conversion_kind conversion);
    [[nodiscard]] static record_assignment prepare_record(takatori::type::data const& source,
        takatori::type::data const& target, meta::field_type const& source_storage, field_position origin,
        meta::field_type const& target_storage, field_position destination, bool nullable,
        conversion_kind conversion);
    [[nodiscard]] status apply(data::any source, accessor::record_ref target,
        request_context* context, memory::lifo_paged_memory_resource* resource) const;
    [[nodiscard]] status apply(accessor::const_record_ref source, accessor::record_ref target,
        request_context* context, memory::lifo_paged_memory_resource* resource) const;
private:
    record_assignment(takatori::type::data const& source, takatori::type::data const& target,
        data::field_operations destination, field_position position, bool nullable, conversion_kind conversion);
    struct record_source { data::field_operations operations; field_position position; };
    takatori::type::data const* source_type_;
    takatori::type::data const* target_type_;
    data::field_operations destination_;
    field_position position_;
    bool nullable_;
    conversion_kind conversion_;
    bool requires_conversion_;
    std::optional<record_source> source_{};
};
}
