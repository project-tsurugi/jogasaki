if(TARGET tbb)
    return()
endif()
find_library(TBB_LIBRARY_FILE NAMES tbb)
find_path(TBB_INCLUDE_DIR NAMES tbb/tbb.h)
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(TBB DEFAULT_MSG
        TBB_LIBRARY_FILE
        TBB_INCLUDE_DIR)
if(TBB_FOUND)
    add_library(tbb SHARED IMPORTED)
    set_target_properties(tbb PROPERTIES
            IMPORTED_LOCATION "${TBB_LIBRARY_FILE}"
            INTERFACE_INCLUDE_DIRECTORIES "${TBB_INCLUDE_DIR}")
endif()
unset(TBB_LIBRARY_FILE CACHE)
unset(TBB_INCLUDE_DIR CACHE)