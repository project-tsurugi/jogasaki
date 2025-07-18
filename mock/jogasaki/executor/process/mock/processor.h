/*
 * Copyright 2018-2024 Project Tsurugi.
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

#include <jogasaki/executor/io/reader_container.h>
#include <jogasaki/executor/io/record_reader.h>
#include <jogasaki/executor/io/record_writer.h>
#include <jogasaki/executor/process/abstract/processor.h>
#include <jogasaki/executor/process/abstract/task_context.h>
#include <jogasaki/executor/process/mock/record_reader.h>
#include <jogasaki/executor/process/step.h>

namespace jogasaki::executor::process::mock {

class processor : public abstract::processor {

    using status = abstract::status;
public:
    [[nodiscard]] status run(abstract::task_context* ctx) override {
        auto* r = ctx->reader(0).reader<io::record_reader>();
        auto* w = ctx->downstream_writer(0);
        auto* e = ctx->external_writer();
        while(r->next_record()) {
            auto rec = r->get_record();
            w->write(rec);
            e->write(rec);
        }
        w->release();
        e->release();
        r->release();
        (void)ctx->release_work();
        return status::completed;
    }
};

}


