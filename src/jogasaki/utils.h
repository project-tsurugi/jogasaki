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

#include <variant>
#include <boost/thread/thread.hpp>
#include <numa.h>

#include <takatori/util/universal_extractor.h>
#include <takatori/util/reference_list_view.h>

#include <jogasaki/model/step.h>
#include <jogasaki/model/port.h>

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

inline bool set_core_affinity(boost::thread* t, std::size_t cpu, bool uniform_on_nodes = false) {
    if (uniform_on_nodes) {
        static std::size_t nodes = numa_max_node()+1;
        return 0 == numa_run_on_node(static_cast<int>(cpu % nodes));
    }
    pthread_t x = t->native_handle();
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu, &cpuset);  //NOLINT
    return 0 == ::pthread_setaffinity_np(x, sizeof(cpu_set_t), &cpuset);
}

template<class T, class Variant, std::size_t index = 0>
constexpr std::size_t alternative_index() noexcept {
    if constexpr (index == std::variant_size_v<Variant>) {
        return -1;
    } else if constexpr (std::is_same_v<std::variant_alternative_t<index, Variant>, T>) {
        return index;
    } else {
        return alternative_index<T, Variant, index + 1>();
    }
}

}

