#pragma once

#include <stdexcept>
#include <utility>

#include "value_object.h"

#include <takatori/descriptor/variable.h>
#include <takatori/descriptor/relation.h>
#include <takatori/descriptor/function.h>
#include <takatori/descriptor/aggregate_function.h>
#include <takatori/descriptor/declared_type.h>

namespace takatori::testing {

template<class T, descriptor::descriptor_kind Kind>
inline T const& value_of(descriptor::element<Kind> const& desc) {
    return testing::value_object<T>::extract(desc.entity());
}

inline descriptor::variable vardesc(int v) {
    return descriptor::variable { std::make_shared<testing::value_object<int>>(v) };
}

inline descriptor::function funcdesc(int v) {
    return descriptor::function { std::make_shared<testing::value_object<int>>(v) };
}

inline descriptor::relation tabledesc(std::string_view name) {
    return descriptor::relation { std::make_shared<testing::value_object<std::string>>(std::string {name}) };
}

inline descriptor::relation exchangedesc(std::string_view name) {
    return descriptor::relation { std::make_shared<testing::value_object<std::string>>(std::string {name}) };
}

inline descriptor::variable columndesc(std::string_view name) {
    return descriptor::variable { std::make_shared<testing::value_object<std::string>>(std::string {name}) };
}

inline descriptor::aggregate_function aggdesc(std::string_view name) {
    return descriptor::aggregate_function { std::make_shared<testing::value_object<std::string>>(std::string {name}) };
}

inline descriptor::declared_type typedesc(std::string_view name) {
    return descriptor::declared_type { std::make_shared<testing::value_object<std::string>>(std::string {name}) };
}

} // namespace takatori::testing
