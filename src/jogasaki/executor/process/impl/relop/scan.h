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
#include <jogasaki/storage/storage_context.h>
#include <jogasaki/storage/transaction_context.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/executor/process/abstract/scan_info.h>
#include "operator_base.h"
#include "scan_context.h"

namespace jogasaki::executor::process::impl::relop {

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
    scan(std::shared_ptr<abstract::scan_info> info,
        std::shared_ptr<meta::record_meta> meta
    ) :
        info_(std::move(info)),
        meta_(std::move(meta))
    {}

    void operator()(scan_context& ctx) {
        if (! ctx.tx_) {
            open(ctx);
        }
        if(!next(ctx)) {
            close(ctx);
        }
    }

    void open(scan_context& ctx) {
        if(! ctx.storage_->open()) {
            fail();
        }
        ctx.tx_ = ctx.storage_->create_transaction();
        ctx.tx_->open_scan();
    }

    bool next(scan_context& ctx) {
        return ctx.tx_->next_scan();
    }

    void close(scan_context& ctx) {
        ctx.tx_->close_scan();
        ctx.tx_->commit();
        ctx.storage_->close();
    }

    operator_kind kind() override {
        return operator_kind::scan;
    }

private:
    std::shared_ptr<abstract::scan_info> info_{};
    std::shared_ptr<meta::record_meta> meta_{};
};

}


