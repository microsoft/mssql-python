set(DDBC_NATIVE_BUILD_CONFIG "${CMAKE_CURRENT_LIST_DIR}/../native_build_config.h")

function(ddbc_native_build_options target)
    if(NOT CMAKE_CXX_COMPILER_ID MATCHES "^(GNU|Clang|AppleClang)$" OR MSVC)
        return()
    endif()

    # Do not downgrade an inherited stack-protector-all setting.
    if(NOT CMAKE_CXX_FLAGS MATCHES "(^| )-fstack-protector-all( |$)")
        set(stronger_configs "")
        foreach(config DEBUG RELEASE RELWITHDEBINFO MINSIZEREL ${CMAKE_CONFIGURATION_TYPES} ${CMAKE_BUILD_TYPE})
            string(TOUPPER "${config}" config_upper)
            if(CMAKE_CXX_FLAGS_${config_upper} MATCHES "(^| )-fstack-protector-all( |$)")
                list(APPEND stronger_configs "$<CONFIG:${config}>")
            endif()
        endforeach()
        if(stronger_configs)
            list(JOIN stronger_configs "," stronger_configs)
            target_compile_options(${target} PRIVATE
                "$<$<NOT:$<OR:${stronger_configs}>>:-fstack-protector-strong>")
        else()
            target_compile_options(${target} PRIVATE -fstack-protector-strong)
        endif()
    endif()

    if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
        # Define the libc default before any system header, without replacing a
        # caller's definition or enabling it for unoptimized debug builds.
        target_compile_options(${target} PRIVATE
            -include "${DDBC_NATIVE_BUILD_CONFIG}")
        target_link_options(${target} PRIVATE
            "LINKER:-z,relro" "LINKER:-z,now" "LINKER:-z,noexecstack")
    endif()
endfunction()
