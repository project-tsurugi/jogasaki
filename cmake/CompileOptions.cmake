set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-omit-frame-pointer")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -fno-omit-frame-pointer")

set(sanitizers "address")
if(ENABLE_UB_SANITIZER)
    # NOTE: UB check requires instrumented libstdc++
    set(sanitizers "${sanitizers},undefined")
endif()
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    # do nothing for gcc
elseif(CMAKE_CXX_COMPILER_ID MATCHES "^(Clang|AppleClang)$")
    set(sanitizers "${sanitizers},nullability")
else()
    message(FATAL_ERROR "unsupported compiler ${CMAKE_CXX_COMPILER_ID}")
endif()

if(ENABLE_SANITIZER)
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=${sanitizers}")
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-sanitize=alignment")
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-sanitize-recover=${sanitizers}")
endif()
if(ENABLE_COVERAGE)
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} --coverage -fprofile-update=atomic")
endif()

function(set_compile_options target_name)
    if (BUILD_STRICT)
        target_compile_options(${target_name}
            PRIVATE -Wall -Wextra -Werror)
    else()
        target_compile_options(${target_name}
            PRIVATE -Wall -Wextra)
    endif()
    if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL 12)
        target_compile_options(${target_name}
            PRIVATE -Wno-dangling-reference -Wno-maybe-uninitialized)
    endif()
endfunction(set_compile_options)

if(TRACY_ENABLE)
    message("trace enabled")
    add_definitions(-DTRACY_ENABLE)
    add_definitions(-DTRACY_NO_SAMPLING)

    # tracy code has many unused variables/parameters
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Wno-unused-parameter -Wno-maybe-uninitialized")
    set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -Wno-unused-parameter -Wno-maybe-uninitialized")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -Wno-unused-parameter -Wno-maybe-uninitialized")
endif()

if(LIKWID_ENABLE)
    message("likwid enabled")
    add_definitions(-DLIKWID_PERFMON)
endif()

if(ENABLE_ALTIMETER)
    message("altimeter enabled")
    add_definitions(-DENABLE_ALTIMETER)
endif()
