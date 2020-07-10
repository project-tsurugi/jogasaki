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
#include <jogasaki/data/record_store.h>
#include <jogasaki/executor/process/abstract/scan_info.h>
#include "operator_base.h"
#include "offer_context.h"

namespace jogasaki::executor::process::impl::relop {

/**
 * @brief offer operator
 */
class offer : public operator_base {
public:
    friend class offer_context;
    /**
     * @brief create empty object
     */
    offer() = default;

    /**
     * @brief create new object
     */
    offer(
        std::shared_ptr<meta::record_meta> meta
    ) :
        meta_(std::move(meta))
    {}

    void operator()(offer_context& ctx) {
        auto rec = ctx.store_.ref();
        // fill destination variables
        if (ctx.writer_) {
            ctx.writer_->write(rec);
        }
    }

    operator_kind kind() override {
        return operator_kind::offer;
    }

    [[nodiscard]] std::shared_ptr<meta::record_meta> const& meta() const noexcept {
        return meta_;
    }
private:
    std::shared_ptr<meta::record_meta> meta_{};
};

}


