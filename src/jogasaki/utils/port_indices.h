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

#include <memory>

#include <takatori/util/sequence_view.h>

#include <jogasaki/model/port.h>
#include <jogasaki/model/step.h>

namespace jogasaki::utils {

[[nodiscard]] model::step::port_index_type find_port_index(
    model::port const& p,
    takatori::util::sequence_view<std::unique_ptr<model::port> const> sv
);

[[nodiscard]] model::step::port_index_type input_port_index(
    model::step const& s,
    model::port const& p
);

[[nodiscard]] model::step::port_index_type subinput_port_index(
    model::step const& s,
    model::port const& p
);

[[nodiscard]] model::step::port_index_type output_port_index(
    model::step const& s,
    model::port const& p
);

}

