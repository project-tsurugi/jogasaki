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

#include <string_view>
#include <atomic>
#include <memory>
#include <dlfcn.h>

#include <glog/logging.h>
#include <takatori/util/fail.h>

#include <jogasaki/api.h>

#include <tateyama/status.h>
#include <tateyama/api/server/service.h>

#include <cstring>

#ifndef JOGASAKI_LIBRARY_NAME
#error missing jogasaki library name
#endif
// a trick to convert defined value to char literal
#define to_string_1(s) #s //NOLINT
#define to_string_2(s) to_string_1(s) //NOLINT
static constexpr auto jogasaki_library_name = to_string_2(JOGASAKI_LIBRARY_NAME); //NOLINT

namespace jogasaki {
class configuration;
namespace api {
class environment;
class database;
}
}

namespace tateyama::utils {

using takatori::util::fail;

namespace details {

/**
 * @brief dynamic library loader
 * @details
 */
class loader {
public:
    loader(std::string_view filename, int flags) :
        filename_(filename),
        flags_(flags),
        handle_(nullptr)
    {
        open();
    }

    loader() {
        close();
    }

    loader(loader const& src) = delete;
    loader(loader&&) = delete;

    loader& operator = (loader const&) = delete;
    loader& operator = (loader&&) = delete;

    void* lookup(std::string_view symbol) {
        void *fn = dlsym(handle_, symbol.data());
        if (nullptr == fn) {
            throw_exception(dlerror());
        }
        return fn;
    }

private:
    std::string filename_;
    int flags_;
    void* handle_;
    std::string error_{};

    void open() {
        handle_ = dlopen(filename_.c_str(), flags_);
        if (nullptr == handle_) {
            throw_exception(dlerror());
        }
        dlerror(); // reset error
    }
    void close() noexcept {
        if (nullptr != handle_) {
            dlclose(handle_);
            handle_ = nullptr;
        }
    }
    void throw_exception(const char* msg) {
        error_.assign(msg);
        throw std::runtime_error(error_);
    }

};

inline loader& get_loader() {
    static loader ldr(jogasaki_library_name, RTLD_NOW);
    return ldr;
}

}

/**
 * @brief load and create environment
 * @details load necessary SQL engine libraries and create environment.
 * This will initialize the environment for SQL engine. Call this first before start using any other jogasaki functions.
 * @note TODO revisit when components boundary is updated
 * @warning ASAN and dlopen has compatibility issue (https://bugs.llvm.org/show_bug.cgi?id=27790).
 * Specify install prefix to LD_LIBRARY_PATH when ASAN is used (e.g. on Debug build).
 */
inline std::shared_ptr<jogasaki::api::environment> create_environment() {
    auto& ldr = details::get_loader();
    jogasaki::api::environment* (*creater)(){};
    void (*deleter)(jogasaki::api::environment*){};
    try {
        creater = reinterpret_cast<decltype(creater)>(ldr.lookup("new_environment"));  //NOLINT
        deleter = reinterpret_cast<decltype(deleter)> (ldr.lookup("delete_environment"));  //NOLINT
        return std::shared_ptr<jogasaki::api::environment>{creater(), [=](jogasaki::api::environment* ptr) { deleter(ptr); }};
    } catch(const std::runtime_error& e) {
        LOG(ERROR) << e.what();
        fail();
    }
}


/**
 * @brief load and create database
 * @details load necessary SQL engine libraries and create database.
 * @param cfg the configuration used to create the database
 * @return the database object
 * @note TODO revisit when components boundary is updated
 * @warning ASAN and dlopen has compatibility issue (https://bugs.llvm.org/show_bug.cgi?id=27790).
 * Specify install prefix to LD_LIBRARY_PATH when ASAN is used (e.g. on Debug build).
 */
inline std::shared_ptr<jogasaki::api::database> create_database(jogasaki::configuration* cfg) {
    auto& ldr = details::get_loader();
    jogasaki::api::database* (*creater)(jogasaki::configuration*){};
    void (*deleter)(jogasaki::api::database*){};
    try {
        creater = reinterpret_cast<decltype(creater)>(ldr.lookup("new_database"));  //NOLINT
        deleter = reinterpret_cast<decltype(deleter)> (ldr.lookup("delete_database"));  //NOLINT
        return std::shared_ptr<jogasaki::api::database>{creater(cfg), [=](jogasaki::api::database* ptr) { deleter(ptr); }};
    } catch(const std::runtime_error& e) {
        LOG(ERROR) << e.what();
        fail();
    }
}


/**
 * @brief load and create application
 * @details load necessary SQL engine libraries and create application
 * @param db the database used by the application
 * @return the application object
 * @note TODO revisit when components boundary is updated
 * @warning ASAN and dlopen has compatibility issue (https://bugs.llvm.org/show_bug.cgi?id=27790).
 * Specify install prefix to LD_LIBRARY_PATH when ASAN is used (e.g. on Debug build).
 */
inline std::shared_ptr<tateyama::api::server::service> create_application(jogasaki::api::database* db) {
    auto& ldr = details::get_loader();
    tateyama::api::server::service* (*creater)(jogasaki::api::database*){};
    void (*deleter)(tateyama::api::server::service*){};
    try {
        creater = reinterpret_cast<decltype(creater)>(ldr.lookup("new_application"));  //NOLINT
        deleter = reinterpret_cast<decltype(deleter)> (ldr.lookup("delete_application"));  //NOLINT
        return std::shared_ptr<tateyama::api::server::service>{creater(db), [=](tateyama::api::server::service* ptr) {
            deleter(ptr);
        }};
    } catch(const std::runtime_error& e) {
        LOG(ERROR) << e.what();
        fail();
    }
}

}










