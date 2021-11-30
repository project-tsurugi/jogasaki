if(TARGET likwid::likwid)
    return()
endif()

find_library(likwid_LIBRARY_FILE NAMES likwid)
find_path(likwid_INCLUDE_DIR NAMES likwid.h)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(likwid DEFAULT_MSG
    likwid_LIBRARY_FILE
    likwid_INCLUDE_DIR)

if(likwid_LIBRARY_FILE AND likwid_INCLUDE_DIR)
    set(likwid_FOUND ON)
    add_library(likwid::likwid SHARED IMPORTED)
    set_target_properties(likwid::likwid PROPERTIES
        IMPORTED_LOCATION "${likwid_LIBRARY_FILE}"
        INTERFACE_INCLUDE_DIRECTORIES "${likwid_INCLUDE_DIR}")
else()
    set(likwid_FOUND OFF)
endif()

unset(likwid_LIBRARY_FILE CACHE)
unset(likwid_INCLUDE_DIR CACHE)
