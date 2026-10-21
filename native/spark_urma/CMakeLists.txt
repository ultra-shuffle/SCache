cmake_minimum_required(VERSION 3.16)
project(spark_urma LANGUAGES CXX C)

if(NOT CMAKE_CONFIGURATION_TYPES AND NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()
set(CMAKE_SKIP_BUILD_RPATH TRUE)
set(CMAKE_SKIP_INSTALL_RPATH TRUE)
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH FALSE)

# ---- Paths ----
set(URMA_ROOT "" CACHE PATH "Optional installed vendor UMDK prefix")
find_path(URMA_INC urma_api.h
    HINTS "${URMA_ROOT}/include"
    PATH_SUFFIXES ub/umdk/urma
    REQUIRED)
set(URMA_COMMON_INC "" CACHE PATH "Unused legacy public-header compatibility variable")
find_library(URMA_LIBRARY NAMES urma
    HINTS "${URMA_ROOT}/lib64" "${URMA_ROOT}/lib" "${URMA_ROOT}/urma/lib/urma/core" REQUIRED)
find_library(URMA_COMMON_LIBRARY NAMES urma_common
    HINTS "${URMA_ROOT}/lib64" "${URMA_ROOT}/lib" "${URMA_ROOT}/urma/common" REQUIRED)
message(STATUS "UMDK public headers: ${URMA_INC}")
message(STATUS "UMDK libraries: ${URMA_LIBRARY};${URMA_COMMON_LIBRARY}")

# Use only the provider header paired with the explicitly selected SDK. The
# B081/June-24 perftest sets standalone+IODIE before creating queues; relying
# on the provider's PORT default tests a different aggregate path. Do not use
# hard-coded user_ctl numbers or the B081 generic context setter (opcode 5).
set(SPARK_URMA_BOND_CONTEXT_API 0)
if(EXISTS "${URMA_INC}/urma_ubagg.h")
    include(CheckCXXSourceCompiles)
    set(CMAKE_REQUIRED_INCLUDES "${URMA_INC}")
    unset(SPARK_URMA_HAS_BOND_CONTEXT CACHE)
    check_cxx_source_compiles("
        #include <urma_api.h>
        #include <urma_ubagg.h>
        int main() {
            bondp_set_bonding_mode_in_t cfg = {};
            cfg.bonding_mode = BONDP_BONDING_MODE_STANDALONE;
            cfg.bonding_level = BONDP_BONDING_LEVEL_IODIE;
            return (int)cfg.bonding_mode + (int)cfg.bonding_level +
                (int)BONDP_USER_CTL_SET_BONDING_MODE;
        }" SPARK_URMA_HAS_BOND_CONTEXT)
    unset(CMAKE_REQUIRED_INCLUDES)
    if(SPARK_URMA_HAS_BOND_CONTEXT)
        set(SPARK_URMA_BOND_CONTEXT_API 1)
    endif()
endif()
set(SPARK_URMA_BOND_CONTEXT_API ${SPARK_URMA_BOND_CONTEXT_API}
    CACHE INTERNAL "Paired SDK explicit bonding-context support" FORCE)
message(STATUS "Paired SDK explicit bonding-context API: ${SPARK_URMA_BOND_CONTEXT_API}")

# ---- JNI headers (the library does not link libjvm/AWT) ----
set(JAVA_HOME "$ENV{JAVA_HOME}" CACHE PATH "JDK root")
if(NOT JAVA_HOME)
    message(FATAL_ERROR "Set JAVA_HOME or -DJAVA_HOME to a JDK root")
endif()
find_path(JNI_INCLUDE_DIR jni.h HINTS "${JAVA_HOME}/include" REQUIRED NO_DEFAULT_PATH)
find_path(JNI_MD_INCLUDE_DIR jni_md.h
    HINTS "${JAVA_HOME}/include/linux" "${JAVA_HOME}/include/darwin"
    REQUIRED NO_DEFAULT_PATH)
set(JNI_INCLUDE_DIRS "${JNI_INCLUDE_DIR}" "${JNI_MD_INCLUDE_DIR}")

# ---- Library ----
set(SOURCES
    src/urma_transport.cpp
    src/tcp_control_channel.cpp
)

set(HEADERS
    include/urma_transport.h
    include/urma_types.h
    include/chunk_protocol.h
    include/transport_metrics.h
)

add_library(spark_urma_obj OBJECT ${SOURCES})
target_include_directories(spark_urma_obj PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${URMA_INC}
    ${URMA_COMMON_INC}
    ${JNI_INCLUDE_DIRS}
)
target_compile_features(spark_urma_obj PUBLIC cxx_std_17)
target_compile_definitions(spark_urma_obj PRIVATE
    SPARK_URMA_BOND_CONTEXT_API=${SPARK_URMA_BOND_CONTEXT_API})
target_compile_options(spark_urma_obj PRIVATE -Wall -Wextra -Werror)
set_target_properties(spark_urma_obj PROPERTIES POSITION_INDEPENDENT_CODE ON)

option(BUILD_STANDALONE "Build standalone C++ transport library" OFF)
if(BUILD_STANDALONE)
    add_library(spark_urma SHARED $<TARGET_OBJECTS:spark_urma_obj>)
    target_link_libraries(spark_urma PRIVATE
        ${URMA_LIBRARY} ${URMA_COMMON_LIBRARY} pthread ${CMAKE_DL_LIBS})
endif()

# JNI shared library (libSparkUrma.so)
add_library(SparkUrma SHARED
    $<TARGET_OBJECTS:spark_urma_obj>
    src/spark_urma_jni.cpp
)
target_include_directories(SparkUrma PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include ${URMA_INC} ${URMA_COMMON_INC} ${JNI_INCLUDE_DIRS})
target_compile_features(SparkUrma PRIVATE cxx_std_17)
target_compile_options(SparkUrma PRIVATE -Wall -Wextra -Werror)
target_link_libraries(SparkUrma PRIVATE
    ${URMA_LIBRARY} ${URMA_COMMON_LIBRARY} pthread ${CMAKE_DL_LIBS}
)
include(GNUInstallDirs)
install(TARGETS SparkUrma LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})

# ---- Tests ----
option(BUILD_TESTS "Build standalone tests" ON)
if(BUILD_TESTS)
    enable_testing()
    foreach(contract local_va_contract_test rm_ctp_dma_lifetime_test rm_ctp_post_ownership_test rm_ctp_mr_import_test rm_ctp_connect_test rm_ctp_init_test registration_diagnostics_test)
        add_executable(${contract} tests/${contract}.cpp $<TARGET_OBJECTS:spark_urma_obj>)
        target_include_directories(${contract} PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/include ${URMA_INC} ${URMA_COMMON_INC})
        target_compile_options(${contract} PRIVATE -Wall -Wextra -Werror)
        target_link_libraries(${contract} PRIVATE
            ${URMA_LIBRARY} ${URMA_COMMON_LIBRARY} pthread ${CMAKE_DL_LIBS})
        add_test(NAME ${contract} COMMAND ${contract})
        set_tests_properties(${contract} PROPERTIES LABELS "offline-contract" TIMEOUT 15)
    endforeach()
    target_link_options(rm_ctp_post_ownership_test PRIVATE
        -Wl,--wrap=urma_post_jetty_send_wr -Wl,--wrap=urma_poll_jfc)
    target_link_options(rm_ctp_mr_import_test PRIVATE
        -Wl,--wrap=urma_import_seg -Wl,--wrap=urma_unimport_seg)
    target_link_options(registration_diagnostics_test PRIVATE
        -Wl,--wrap=urma_alloc_token_id -Wl,--wrap=urma_register_seg -Wl,--wrap=urma_free_token_id)
    target_link_options(rm_ctp_connect_test PRIVATE
        -Wl,--wrap=urma_import_jetty -Wl,--wrap=urma_unimport_jetty
        -Wl,--wrap=urma_bind_jetty -Wl,--wrap=urma_unbind_jetty)
    add_executable(jni_pin_registration_failure_test
        tests/jni_pin_registration_failure_test.cpp $<TARGET_OBJECTS:spark_urma_obj>)
    target_include_directories(jni_pin_registration_failure_test PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include ${URMA_INC} ${URMA_COMMON_INC} ${JNI_INCLUDE_DIRS})
    target_compile_options(jni_pin_registration_failure_test PRIVATE -Wall -Wextra -Werror)
    target_link_libraries(jni_pin_registration_failure_test PRIVATE
        ${URMA_LIBRARY} ${URMA_COMMON_LIBRARY} pthread ${CMAKE_DL_LIBS})
    target_link_options(jni_pin_registration_failure_test PRIVATE
        -Wl,--wrap=_ZN10spark_urma13UrmaTransport4waitEmj)
    add_test(NAME jni_pin_registration_failure_test COMMAND jni_pin_registration_failure_test)
    set_tests_properties(jni_pin_registration_failure_test PROPERTIES LABELS "offline-contract" TIMEOUT 15)
    foreach(api init uninit get_device_by_name query_device get_eid_list free_eid_list
            create_context create_jfc create_jfr create_jetty
            delete_context delete_jfc delete_jfr delete_jetty)
        target_link_options(rm_ctp_init_test PRIVATE "-Wl,--wrap=urma_${api}")
    endforeach()
    target_compile_definitions(rm_ctp_init_test PRIVATE
        SPARK_URMA_BOND_CONTEXT_API=${SPARK_URMA_BOND_CONTEXT_API})
    target_link_options(rm_ctp_init_test PRIVATE -Wl,--wrap=urma_user_ctl)
    add_executable(native_lifecycle_test tests/native_lifecycle_test.cpp src/chunk_protocol.cpp
        $<TARGET_OBJECTS:spark_urma_obj>)
    target_include_directories(native_lifecycle_test PRIVATE
        ${URMA_INC} ${URMA_COMMON_INC} ${CMAKE_CURRENT_SOURCE_DIR}/include)
    target_link_libraries(native_lifecycle_test PRIVATE
        ${URMA_LIBRARY} ${URMA_COMMON_LIBRARY} pthread)
    add_executable(request_state_test tests/request_state_test.cpp
        $<TARGET_OBJECTS:spark_urma_obj>)
    target_include_directories(request_state_test PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include ${URMA_INC} ${URMA_COMMON_INC})
    target_compile_options(request_state_test PRIVATE -Wall -Wextra -Werror)
    target_link_libraries(request_state_test PRIVATE
        ${URMA_LIBRARY} ${URMA_COMMON_LIBRARY} pthread)
    # Pure state-machine test: no URMA device or provider initialization.
    add_test(NAME request_state_test COMMAND request_state_test)
    set_tests_properties(request_state_test PROPERTIES LABELS "offline-contract" TIMEOUT 15)
    add_executable(remote_import_cache_test tests/remote_import_cache_test.cpp
        $<TARGET_OBJECTS:spark_urma_obj>)
    target_include_directories(remote_import_cache_test PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include ${URMA_INC} ${URMA_COMMON_INC})
    target_compile_options(remote_import_cache_test PRIVATE -Wall -Wextra -Werror)
    target_link_libraries(remote_import_cache_test PRIVATE
        ${URMA_LIBRARY} ${URMA_COMMON_LIBRARY} pthread)
endif()
