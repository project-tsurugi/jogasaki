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

#include <jogasaki/executor/process/step.h>
#include <jogasaki/executor/reader_container.h>
#include <jogasaki/executor/record_writer.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/transaction.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/executor/process/abstract/scan_info.h>
#include "operator_base.h"
#include "scan_context.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::maybe_shared_ptr;

/**
 * @brief scanner
 */
class scan : public operator_base {
public:
    friend class scan_context;
    /**
     * @brief create empty object
     */
    scan() = default;

    /**
     * @brief create new object
     */
    scan(
        processor_info const& info,
        block_index_type block_index,
        std::shared_ptr<abstract::scan_info> scan_info,
        maybe_shared_ptr<meta::record_meta> meta,
        relation::expression const* downstream
    ) : operator_base(info, block_index),
        info_(std::move(scan_info)),
        meta_(std::move(meta)),
        downstream_(downstream)
    {}

    void operator()(scan_context& ctx, operator_executor* visitor = nullptr) {
        open(ctx);
        while(ctx.it_ && ctx.it_->next()) { // TODO assume ctx.it_ always exist
            // TODO implement
            if (visitor) {
                dispatch(*visitor, *downstream_);
            }
        }
        close(ctx);
    }

    void open(scan_context& ctx) {
        if (ctx.stg_ && ctx.tx_ && !ctx.it_) {
            if(auto res = ctx.stg_->scan(*ctx.tx_, "", kvs::end_point_kind::unbound, "", kvs::end_point_kind::unbound, ctx.it_);
                !res) {
                fail();
            }
        }
    }

    void close(scan_context& ctx) {
        ctx.it_.release();
    }

    [[nodiscard]] operator_kind kind() const noexcept override {
        return operator_kind::scan;
    }

private:
    std::shared_ptr<abstract::scan_info> info_{};
    maybe_shared_ptr<meta::record_meta> meta_{};
    relation::expression const* downstream_{};
};

}


