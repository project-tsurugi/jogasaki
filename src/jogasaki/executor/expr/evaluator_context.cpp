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
#include "evaluator_context.h"

namespace jogasaki::executor::expr {

std::pair<std::string, std::string> create_conversion_error_message(evaluator_context const& ctx) {
    if(ctx.errors().empty()) {
        return {"<no message>", {}};
    }
    std::stringstream ss{};
    auto& err = ctx.errors().front();
    if(! err.arguments().empty()) {
        auto sz = err.arguments().size();
        ss << "source_value:{" << err.arguments().front().str() << "} ";
        if (sz > 1) {
            ss << "computed_values:[";
                for(std::size_t i=0; i < sz - 1; ++i) {
                    if(i != 0) {
                        ss << ",";
                    }
                    ss << "{" << err.arguments()[i].str() << "}";
                }
            ss << "]";
        }
    }
    std::stringstream ms{};
    ms << err.code() << ": " << err.message();
    return {ms.str(), ss.str()};
}

}  // namespace jogasaki::executor::expr
