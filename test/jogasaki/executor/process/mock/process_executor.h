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

#include <jogasaki/executor/process/step.h>
#include <jogasaki/executor/process/processor_context.h>
#include <jogasaki/executor/process/processor.h>

namespace jogasaki::executor::process::mock {

class process_executor {
public:
    process_executor(std::shared_ptr<processor> processor, std::shared_ptr<processor_context> context) :
            processor_(std::move(processor)), context_(std::move(context)) {}

    bool run() {
        processor_->context(context_);
        processor_->run();
        return true;
    }
private:
    std::shared_ptr<processor> processor_{};
    std::shared_ptr<processor_context> context_{};
};

}


