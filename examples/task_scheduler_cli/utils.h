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

#include <iomanip>
#include <locale>
#include <ostream>
#include <sstream>

namespace tateyama::task_scheduler_cli {

// width setter for ostream
struct cwidth {
    cwidth() : cwidth(30) {}
    cwidth(std::size_t n) : n_(n) {}
    std::size_t n_;
};

inline std::ostream& operator<<(std::ostream& out, cwidth in) {
    return out << std::right << std::setw(in.n_) << std::setfill(' ');
}

template<class T>
std::string format(T value){
    struct punct : public std::numpunct<char>{
    protected:
        virtual char do_thousands_sep() const {return ',';}
        virtual std::string do_grouping() const {return "\03";}
    };
    std::stringstream ss;
    ss.imbue({std::locale(), new punct});
    ss << std::setprecision(2) << std::fixed << value;
    return ss.str();
}

}

