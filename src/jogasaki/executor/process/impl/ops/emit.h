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

#include <vector>

#include <takatori/util/sequence_view.h>
#include <takatori/util/object_creator.h>
#include <takatori/relation/emit.h>
#include <takatori/descriptor/variable.h>

#include <jogasaki/executor/process/step.h>
#include <jogasaki/executor/reader_container.h>
#include <jogasaki/executor/record_writer.h>
#include <jogasaki/data/record_store.h>
#include <jogasaki/executor/process/abstract/scan_info.h>
#include <jogasaki/executor/process/impl/block_scope.h>
#include <jogasaki/utils/copy_field_data.h>
#include "operator_base.h"
#include "emit_context.h"

namespace jogasaki::executor::process::impl::ops {


namespace details {

struct emit_field {
    meta::field_type type_{};
    std::size_t source_offset_{};
    std::size_t target_offset_{};
    std::size_t source_nullity_offset_{};
    std::size_t target_nullity_offset_{};
    bool nullable_{};
};

}

/**
 * @brief emit operator
 */
class emit : public operator_base {
public:
    friend class emit_context;

    using column = takatori::relation::emit::column;

    /**
     * @brief create empty object
     */
    emit() = default;

    /**
     * @brief create new object
     */
    explicit emit(
        processor_info const& info,
        block_index_type block_index,
        std::vector<column, takatori::util::object_allocator<column>> const& columns
    ) : operator_base(info, block_index),
        meta_(create_meta(info, columns)),
        fields_(create_fields(meta_, columns))
    {}

    void operator()(emit_context& ctx) {
        auto target = ctx.store_.ref();
        auto source = ctx.variables().store().ref();
        for(auto &f : fields_) {
            utils::copy_field(f.type_, target, f.target_offset_, source, f.source_offset_);
        }

        if (ctx.writer_) {
            // FIXME fetch writer when needed
            ctx.writer_->write(target);
        }
    }

    operator_kind kind() const noexcept override {
        return operator_kind::emit;
    }

    [[nodiscard]] std::shared_ptr<meta::record_meta> const& meta() const noexcept {
        return meta_;
    }

    void external_writer_index(std::size_t index) noexcept {
        external_writer_index_ = index;
    }
private:
    std::shared_ptr<meta::record_meta> meta_{};
    std::vector<details::emit_field> fields_{};
    std::size_t external_writer_index_{};

    std::shared_ptr<meta::record_meta> create_meta(
        processor_info const& info,
        std::vector<column, takatori::util::object_allocator<column>> const& columns
    ) {
        // FIXME currently respect the column order coming from takatori
        std::vector<meta::field_type> fields{};
        auto sz = columns.size();
        fields.reserve(sz);
        for(auto&& c : columns) {
            fields.emplace_back(utils::type_for(info.compiled_info(), c.source()));
        }
        return std::make_shared<meta::record_meta>(std::move(fields), boost::dynamic_bitset<std::uint64_t>(sz)); // TODO nullity
    }

    std::vector<details::emit_field> create_fields(
        std::shared_ptr<meta::record_meta> const& meta,
        std::vector<column, takatori::util::object_allocator<column>> const& columns
    ) {
        std::vector<details::emit_field> fields{};
        std::size_t sz = meta->field_count();
        fields.resize(sz);
        for(std::size_t ind = 0 ; ind < sz; ++ind) {
            auto&& c = columns[ind];
            auto& info = blocks().at(block_index()).value_map().at(c.source());
            fields.emplace_back(details::emit_field{
                meta_->at(ind),
                info.value_offset(),
                meta_->value_offset(ind),
                info.nullity_offset(),
                meta_->nullity_offset(ind),
                //TODO nullity
                false // nullable
            });
        }
        return fields;
    }
};

}


