if(TARGET gperftools::gperftools)
    return()
endif()

find_library(gperftools_LIBRARY_FILE NAMES profiler)
find_path(gperftools_INCLUDE_DIR NAMES gperftools/profiler.h)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(gperftools DEFAULT_MSG
    gperftools_LIBRARY_FILE
    gperftools_INCLUDE_DIR)

if(gperftools_LIBRARY_FILE AND gperftools_INCLUDE_DIR)
    set(gperftools_FOUND ON)
    add_library(gperftools::gperftools SHARED IMPORTED)
    set_target_properties(gperftools::gperftools PROPERTIES
        IMPORTED_LOCATION "${gperftools_LIBRARY_FILE}"
        INTERFACE_INCLUDE_DIRECTORIES "${gperftools_INCLUDE_DIR}")
else()
    set(gperftools_FOUND OFF)
endif()

unset(gperftools_LIBRARY_FILE CACHE)
unset(gperftools_INCLUDE_DIR CACHE)
