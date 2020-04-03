function(install_custom target_name export_name)
    install(
        TARGETS
            ${target_name}
        EXPORT
            ${export_name}
        LIBRARY
            DESTINATION ${CMAKE_INSTALL_LIBDIR}
            COMPONENT Runtime
        ARCHIVE
            DESTINATION ${CMAKE_INSTALL_LIBDIR}/${export_name}
            COMPONENT Development
        RUNTIME
            DESTINATION ${CMAKE_INSTALL_BINDIR}
            COMPONENT Runtime
    )
    # Add INSTALL_RPATH from CMAKE_INSTALL_PREFIX and CMAKE_PREFIX_PATH
    # The default behavior of CMake omits RUNPATH if it is already in CMAKE_CXX_IMPLICIT_LINK_DIRECTORIES.
    if (FORCE_INSTALL_RPATH)
        get_target_property(target_type ${target_name} TYPE)
        if (target_type STREQUAL "SHARED_LIBRARY"
                OR target_type STREQUAL "EXECUTABLE")
            get_target_property(rpath ${target_name} INSTALL_RPATH)

            # add ${CMAKE_INSTALL_PREFIX}/lib if it is not in system link directories
            get_filename_component(p "${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}" ABSOLUTE)
            list(FIND CMAKE_PLATFORM_IMPLICIT_LINK_DIRECTORIES "${p}" is_system)
            if (is_system STREQUAL "-1")
                list(APPEND rpath "${p}")
            endif()

            # add each ${CMAKE_PREFIX_PATH}/lib
            foreach (p IN LISTS CMAKE_PREFIX_PATH)
                get_filename_component(p "${p}/${CMAKE_INSTALL_LIBDIR}" ABSOLUTE)
                list(APPEND rpath "${p}")
            endforeach()

            if (rpath)
                set_target_properties(${target_name} PROPERTIES
                    INSTALL_RPATH "${rpath}")
            endif()

            # add other than */lib paths
            set_target_properties(${target_name} PROPERTIES
                INSTALL_RPATH_USE_LINK_PATH ON)
        endif()
    endif (FORCE_INSTALL_RPATH)
    # Install include files of interface libraries manually
    # INTERFACE_INCLUDE_DIRECTORIES must contains the following entries:
    # - one or more `$<BUILD_INTERFACE:...>` paths (may be absolute paths on source-tree)
    # - just one `$<INSTALL_INTERFACE:...>` path (must be a relative path from the install prefix)
    # then, this copies files in the BUILD_INTERFACE paths onto INSTALL_INTERFACE path
    # e.g.
    #   add_library(shakujo-interface INTERFACE)
    #   target_include_directories(shakujo-interface INTERFACE
    #       $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    #       $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/shakujo>)
    get_target_property(
        _includes
        ${target_name} INTERFACE_INCLUDE_DIRECTORIES
    )
    if (_includes)
        unset(_build_dir)
        unset(_install_dir)
        foreach (f IN LISTS _includes)
            if (f MATCHES "^\\$<BUILD_INTERFACE:(.+)>$")
                list(APPEND _build_dir ${CMAKE_MATCH_1})
            elseif (f MATCHES "^\\$<INSTALL_INTERFACE:(.+)>$")
                set(_install_dir ${CMAKE_MATCH_1})
            else()
                message(FATAL_ERROR "invalid include specification (${target_name}): ${f}")
            endif()
        endforeach()
        if (NOT _build_dir)
            message(FATAL_ERROR "${target_name} must declare \$<BUILD_INTERFACE:...> in INTERFACE_INCLUDE_DIRECTORIES")
        endif()
        if (NOT _install_dir)
            message(FATAL_ERROR "${target_name} must declare \$<INSTALL_INTERFACE:...> in INTERFACE_INCLUDE_DIRECTORIES")
        endif()
        install(
            DIRECTORY ${_build_dir}/
            DESTINATION ${_install_dir}
            COMPONENT Development
            PATTERN "doxygen.h" EXCLUDE
        )
    endif()
endfunction(install_custom)
