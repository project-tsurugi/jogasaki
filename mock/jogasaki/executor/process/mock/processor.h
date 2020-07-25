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
#include <jogasaki/executor/process/abstract/processor.h>
#include <jogasaki/executor/process/mock/record_reader.h>

namespace jogasaki::executor::process::mock {

class processor : public abstract::processor {

    using status = abstract::status;
public:
    status run(abstract::task_context* ctx) override {
        auto* r = ctx->reader(0).reader<executor::record_reader>();
        auto meta = unwrap_record_reader(r)->meta(); // FIXME
        auto* w = ctx->downstream_writer(0);
        auto* e = ctx->external_writer(0);
        while(r->next_record()) {
            auto rec = r->get_record();
            w->write(rec);
            e->write(rec);
        }
        ctx->release_work();
        return status::completed;
    }
};

}


