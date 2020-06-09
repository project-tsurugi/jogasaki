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
#include "port_indices.h"

namespace jogasaki::utils {

model::step::port_index_type
find_port_index(const model::port &p, takatori::util::sequence_view<const std::unique_ptr<model::port>> sv) {
    for(model::step::port_index_type i=0, n = sv.size(); i < n; ++i) {
        if(sv[i].get() == &p) {
            return i;
        }
    }
    throw std::domain_error("port not found");
}

model::step::port_index_type input_port_index(const model::step &s, const model::port &p) {
    return find_port_index(p, s.input_ports());
}

model::step::port_index_type subinput_port_index(const model::step &s, const model::port &p) {
    return find_port_index(p, s.subinput_ports());
}

model::step::port_index_type output_port_index(const model::step &s, const model::port &p) {
    return find_port_index(p, s.output_ports());
}
}

