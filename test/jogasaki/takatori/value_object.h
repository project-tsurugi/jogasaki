#pragma once

#include <stdexcept>
#include <utility>

#include <takatori/util/object.h>
#include <takatori/util/downcast.h>

namespace takatori::testing {

template<class T>
class value_object : public util::object {
public:
    explicit value_object(T value) noexcept : value_(std::move(value)) {}
    T& value() noexcept { return value_; }
    T const& value() const noexcept { return value_; }

    static T& extract(util::object& object) {
        return util::downcast<value_object<T>>(object).value();
    }

    static T const& extract(util::object const& object) {
        return util::downcast<value_object<T>>(object).value();
    }

protected:
    bool equals(object const& other) const noexcept override {
        if (auto* ptr = util::downcast<value_object>(&other)) {
            return value_ == ptr->value_;
        }
        return false;
    }
    std::size_t hash() const noexcept override {
        return std::hash<T>{}(value_);
    }
    std::ostream& print_to(std::ostream& out) const override {
        return out << value_;
    }
private:
    T value_;
};

} // namespace takatori::testing
