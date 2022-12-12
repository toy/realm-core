function(realm_acquire_dependency dep_name dep_version out_dep_cmake)
    set(_target_architecture_Android_armeabi-v7a arm)
    set(_target_architecture_Android_arm64-v8a arm64)
    set(_target_architecture_Android_x86 x86)
    set(_target_architecture_Android_x86_64 x64)
    set(_target_architecture_Windows_Win32 x86)
    set(_target_architecture_Windows_x64 x64)
    set(_target_architecture_Windows_ARM64 arm64)
    set(_target_architecture_WindowsStore_Win32 x86)
    set(_target_architecture_WindowsStore_x64 x64)
    set(_target_architecture_WindowsStore_ARM arm)
    set(_target_architecture_WindowsStore_ARM64 arm64)
    set(_target_architecture_Linux_x86_64 x64)
    set(_target_architecture_Linux_arm arm)
    set(_target_architecture_Linux_armv7 arm)
    set(_target_architecture_Linux_armv7l arm)
    set(_target_architecture_Linux_aarch64 arm64)
    set(_target_platform_name_Android android)
    set(_target_platform_name_Windows windows-static)
    set(_target_platform_name_WindowsStore uwp-static)
    set(_target_platform_name_Linux linux-gnu)

    if(ANDROID)
        set(_target_architecture ${CMAKE_ANDROID_ARCH_ABI})
    elseif(WIN32 AND CMAKE_GENERATOR MATCHES "^Visual Studio")
        set(_target_architecture ${CMAKE_GENERATOR_PLATFORM})
    else()
        set(_target_architecture ${CMAKE_SYSTEM_PROCESSOR})
    endif()

    if(NOT EXISTS ${CMAKE_CURRENT_BINARY_DIR}/${dep_name}/include.cmake)
        set(DEP_URL "https://static.realm.io/downloads/dependencies/${dep_name}/${dep_version}/${dep_name}-${dep_version}-${_target_architecture_${CMAKE_SYSTEM_NAME}_${_target_architecture}}-${_target_platform_name_${CMAKE_SYSTEM_NAME}}.tar.gz")
        message(STATUS "Getting ${DEP_URL}...")
        file(DOWNLOAD "${DEP_URL}" "${CMAKE_CURRENT_BINARY_DIR}/${dep_name}/${dep_name}.tar.gz" STATUS download_status)

        list(GET download_status 0 status_code)
        if (NOT "${status_code}" STREQUAL "0")
            message(FATAL_ERROR "Downloading ${url}... Failed. Status: ${download_status}")
        endif()

        message(STATUS "Uncompressing ${dep_name}...")
        execute_process(
            COMMAND ${CMAKE_COMMAND} -E tar xfz "${dep_name}.tar.gz"
            WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/${dep_name}"
        )
    endif()

    set(${out_dep_cmake} ${CMAKE_CURRENT_BINARY_DIR}/${dep_name}/include.cmake PARENT_SCOPE)
endfunction()