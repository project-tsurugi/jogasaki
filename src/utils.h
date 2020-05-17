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

#include <boost/thread/thread.hpp>

#include <takatori/util/universal_extractor.h>
#include <takatori/util/reference_list_view.h>

#include <model/step.h>
#include <model/port.h>

namespace jogasaki {

inline model::step::port_index_type find_port_index(model::port const& p, takatori::util::sequence_view<std::unique_ptr<model::port> const> sv) {
    for(model::step::port_index_type i=0, n = sv.size(); i < n; ++i) {
        if(sv[i].get() == &p) {
            return i;
        }
    }
    throw std::domain_error("port not found");
}

inline model::step::port_index_type input_port_index(model::step const& s, model::port const& p) {
    return find_port_index(p, s.input_ports());
}

inline model::step::port_index_type subinput_port_index(model::step const& s, model::port const& p) {
    return find_port_index(p, s.subinput_ports());
}

inline model::step::port_index_type output_port_index(model::step const& s, model::port const& p) {
    return find_port_index(p, s.output_ports());
}

inline bool set_core_affinity(boost::thread* t, std::size_t cpu) {
    pthread_t x = t->native_handle();
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu, &cpuset);
    return 0 == ::pthread_setaffinity_np(x, sizeof(cpu_set_t), &cpuset);
}

/*
template <typename T, typename U>
takatori::util::reference_list_view<takatori::util::universal_extractor<T>> reference_list_from_pointer_vector(std::vector<std::unique_ptr<U>>& vp) {
    takatori::util::universal_extractor<T> ext {
            [](void* cursor) -> T& {
                return *dynamic_cast<T*>(static_cast<std::unique_ptr<U>*>(cursor)->get());
            },
            [](void* cursor, std::ptrdiff_t offset) {
                return static_cast<void*>(static_cast<std::unique_ptr<U>*>(cursor) + offset);
            },
    };
    return takatori::util::reference_list_view<takatori::util::universal_extractor<T>>{ vp, ext };
}
 */

}

