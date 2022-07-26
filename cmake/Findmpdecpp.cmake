if(TARGET mpdecpp)
    return()
endif()

find_path(mpdecpp_INCLUDE_DIR NAMES decimal.hh)
find_library(mpdecpp_LIBRARY_FILE NAMES mpdec++)
find_library(mpdec_LIBRARY_FILE NAMES mpdec)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(mpdecpp DEFAULT_MSG
        mpdecpp_INCLUDE_DIR
        mpdecpp_LIBRARY_FILE
        mpdec_LIBRARY_FILE
        )

if(mpdecpp_INCLUDE_DIR AND mpdecpp_LIBRARY_FILE AND mpdec_LIBRARY_FILE)
    set(mpdecpp_FOUND ON)
    add_library(mpdecpp SHARED IMPORTED)
    target_link_libraries(mpdecpp
            INTERFACE "${mpdec_LIBRARY_FILE}"
            )
    set_target_properties(mpdecpp PROPERTIES
        IMPORTED_LOCATION "${mpdecpp_LIBRARY_FILE}"
        INTERFACE_INCLUDE_DIRECTORIES "${mpdecpp_INCLUDE_DIR}")
else()
    set(mpdecpp_FOUND OFF)
endif()

unset(mpdecpp_INCLUDE_DIR CACHE)
unset(mpdecpp_LIBRARY_FILE CACHE)
unset(mpdec_LIBRARY_FILE CACHE)
