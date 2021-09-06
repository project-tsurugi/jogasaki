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

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <boost/dynamic_bitset.hpp>

#include <takatori/type/int.h>
#include <takatori/type/float.h>
#include <takatori/type/character.h>
#include <takatori/value/int.h>
#include <takatori/value/float.h>
#include <takatori/value/character.h>
#include <takatori/util/string_builder.h>
#include <takatori/util/downcast.h>
#include <takatori/relation/emit.h>
#include <takatori/relation/graph.h>
#include <takatori/relation/scan.h>
#include <takatori/relation/filter.h>
#include <takatori/relation/project.h>
#include <takatori/statement/write.h>
#include <takatori/statement/execute.h>
#include <takatori/scalar/immediate.h>
#include <takatori/plan/process.h>
#include <takatori/serializer/json_printer.h>
#include <takatori/util/string_builder.h>
#include <takatori/util/fail.h>
#include <takatori/scalar/variable_reference.h>

#include <yugawara/binding/factory.h>

#include <yugawara/compiler_result.h>

namespace jogasaki::testing {

namespace t = ::takatori::type;
namespace v = ::takatori::value;
namespace descriptor = ::takatori::descriptor;
namespace scalar = ::takatori::scalar;
namespace relation = ::takatori::relation;
namespace plan = ::takatori::plan;
namespace binding = ::yugawara::binding;

using ::takatori::util::downcast;
using ::takatori::util::string_builder;

using varref = scalar::variable_reference;

using ::takatori::util::fail;

template<class T, class Port>
inline T& next(Port& port) {
    if (!port.opposite()) {
        throw std::domain_error("not connected");
    }
    auto&& r = port.opposite()->owner();
    if (r.kind() != T::tag) {
        throw std::domain_error(takatori::util::string_builder {}
                << r.kind()
                << " <=> "
                << T::tag
                << takatori::util::string_builder::to_string);
    }
    return takatori::util::downcast<T>(r);
}

template<class T>
inline T& last(::takatori::relation::graph_type const& graph) {
    for (auto&& e : graph) {
        if (e.output_ports().empty()) {
            return takatori::util::downcast<T>(e);
        }
    }
    fail();
}

template<class T>
inline T const& head(::takatori::relation::graph_type const& graph) {
    T const* result = nullptr;
    ::takatori::relation::enumerate_top(graph, [&](::takatori::relation::expression const& v) {
        result = takatori::util::downcast<T>(&v);
    });
    if (result != nullptr) {
        return *result;
    }
    throw std::domain_error(takatori::util::string_builder {}
            << "missing head: "
            << T::tag
            << takatori::util::string_builder::to_string);
}

inline takatori::plan::process const&
top(takatori::plan::graph_type const& g) {
    takatori::plan::process const* ret{};
    takatori::plan::enumerate_top(g, [&](takatori::plan::step const& s) {
        if (s.kind() == takatori::plan::step_kind::process) {
            if (!ret) {
                ret = takatori::util::downcast<takatori::plan::process>(&s);
            }
        }
    });
    if (!ret) {
        throw std::domain_error(takatori::util::string_builder {}
            << "not found. "
            << takatori::util::string_builder::to_string);
    }
    return *ret;
}

inline takatori::plan::process const&
next_top(takatori::plan::graph_type const& g, takatori::plan::process const& p) {
    takatori::plan::process const* ret{};
    bool prev_found = false;
    takatori::plan::enumerate_top(g, [&](takatori::plan::step const& s) {
        if (s.kind() == takatori::plan::step_kind::process) {
            if (prev_found && !ret) {
                ret = takatori::util::downcast<takatori::plan::process>(&s);
            }
            if (takatori::util::downcast<takatori::plan::process>(s) == p) {
                prev_found = true;
            }
        }
    });
    if (!ret) {
        throw std::domain_error(takatori::util::string_builder {}
            << "not found. "
            << takatori::util::string_builder::to_string);
    }
    return *ret;
}

template<class T>
inline T const& next_relation(::takatori::relation::expression const& v) {
    T const* result = nullptr;
    ::takatori::relation::enumerate_downstream(v, [&](::takatori::relation::expression const& v) {
        result = takatori::util::downcast<T>(&v);
    });
    if (result != nullptr) {
        return *result;
    }
    throw std::domain_error(takatori::util::string_builder {}
        << "missing next "
        << T::tag
        << takatori::util::string_builder::to_string);
}

inline takatori::plan::process&
find(takatori::plan::graph_type const& g, takatori::relation::expression const& e) {
    for (auto&& s : g) {
        if (s.kind() == takatori::plan::step_kind::process) {
            auto&& p = takatori::util::downcast<takatori::plan::process>(s);
            if (p.operators().contains(e)) {
                return p;
            }
        }
    }
    throw std::domain_error(takatori::util::string_builder {}
            << "missing process that contain: "
            << e
            << takatori::util::string_builder::to_string);
}

static void dump(yugawara::compiled_info const& r, takatori::statement::statement const& stmt) {
    ::takatori::serializer::json_printer printer { std::cout };
    r.object_scanner()(
            stmt,
            ::takatori::serializer::json_printer { std::cout });
}

}
