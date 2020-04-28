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

#include <memory>
#include <glog/logging.h>

#include <model/task.h>
#include <model/step.h>
#include <executor/common/task.h>
#include <channel.h>
#include "task_base.h"

namespace jogasaki::executor {

class consumer_task : public task_base {
public:
    consumer_task() = default;
    consumer_task(channel* channel, model::step* src) : task_base(channel, src) {}
    void execute() override {
        DVLOG(1) << *this << " consumer_task executed. count: " << count_;
    }
};

}



