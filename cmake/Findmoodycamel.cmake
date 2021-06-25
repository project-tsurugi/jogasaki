if(TARGET moodycamel)
    return()
endif()

find_path(moodycamel_INCLUDE_DIR NAMES concurrentqueue/concurrentqueue.h)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(moodycamel DEFAULT_MSG
    moodycamel_INCLUDE_DIR)

if(moodycamel_INCLUDE_DIR)
    set(moodycamel_FOUND ON)
    add_library(moodycamel INTERFACE)
    set_target_properties(moodycamel PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES "${moodycamel_INCLUDE_DIR}")
else()
    set(moodycamel_FOUND OFF)
endif()

unset(moodycamel_INCLUDE_DIR CACHE)
