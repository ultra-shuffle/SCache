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

# June-24/B103 context layout. Newer SDKs use a different user-info ABI:
# never reinterpret one layout as the other or silently use the legacy path.
include(CheckCXXSourceCompiles)
set(CMAKE_REQUIRED_INCLUDES "${URMA_INC}")
set(CMAKE_REQUIRED_LIBRARIES "${URMA_LIBRARY};${URMA_COMMON_LIBRARY}")
unset(SPARK_URMA_JUNE_CONTEXT_API CACHE)
check_cxx_source_compiles("
    #include <urma_api.h>
    int main() {
        urma_rjetty_t j = {}; urma_seg_t s = {};
        j.ext.flag.bs.enable = 1; s.ext.flag.bs.enable = 1;
        auto volatile a = &urma_get_rjetty; auto volatile b = &urma_put_rjetty;
        auto volatile c = &urma_get_seg_ctx; auto volatile d = &urma_put_seg_ctx;
        return a == nullptr || b == nullptr || c == nullptr || d == nullptr;
    }" SPARK_URMA_JUNE_CONTEXT_API)
unset(CMAKE_REQUIRED_INCLUDES)
unset(CMAKE_REQUIRED_LIBRARIES)
file(SHA256 "${URMA_INC}/urma_api.h" URMA_API_SHA)
file(SHA256 "${URMA_INC}/urma_types.h" URMA_TYPES_SHA)
string(SHA256 SPARK_URMA_SDK_FINGERPRINT "${URMA_API_SHA}:${URMA_TYPES_SHA}:${CMAKE_SYSTEM_PROCESSOR}")

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
    SPARK_URMA_BOND_CONTEXT_API=${SPARK_URMA_BOND_CONTEXT_API}
    SPARK_URMA_JUNE_CONTEXT_API=$<BOOL:${SPARK_URMA_JUNE_CONTEXT_API}>
    SPARK_URMA_SDK_FINGERPRINT="${SPARK_URMA_SDK_FINGERPRINT}")
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
    if(SPARK_URMA_JUNE_CONTEXT_API)
        add_executable(official_api_alignment_test tests/official_api_alignment_test.cpp $<TARGET_OBJECTS:spark_urma_obj>)
        target_include_directories(official_api_alignment_test PRIVATE include "${URMA_INC}")
        target_compile_definitions(official_api_alignment_test PRIVATE SPARK_URMA_BOND_CONTEXT_API=${SPARK_URMA_BOND_CONTEXT_API})
        target_compile_options(official_api_alignment_test PRIVATE -Wall -Wextra -Werror)
        target_link_libraries(official_api_alignment_test PRIVATE ${URMA_LIBRARY} ${URMA_COMMON_LIBRARY} pthread ${CMAKE_DL_LIBS})
        foreach(api init uninit get_device_by_name query_device get_eid_list free_eid_list create_context user_ctl
            create_jfc create_jfr create_jetty alloc_token_id register_seg get_rjetty put_rjetty get_seg_ctx put_seg_ctx
            import_jetty bind_jetty unbind_jetty import_seg post_jetty_send_wr poll_jfc unimport_seg unimport_jetty
            unregister_seg free_token_id delete_jetty delete_jfr delete_jfc delete_context)
            target_link_options(official_api_alignment_test PRIVATE "-Wl,--wrap=urma_${api}")
        endforeach()
        add_test(NAME official_api_alignment_test COMMAND official_api_alignment_test)
        set_tests_properties(official_api_alignment_test PROPERTIES LABELS "offline-api-alignment" TIMEOUT 15)
        add_executable(provider_context_test tests/provider_context_test.cpp $<TARGET_OBJECTS:spark_urma_obj>)
        target_include_directories(provider_context_test PRIVATE include src "${URMA_INC}")
        target_compile_options(provider_context_test PRIVATE -Wall -Wextra -Werror)
        target_compile_definitions(provider_context_test PRIVATE SPARK_URMA_JUNE_CONTEXT_API=1
            SPARK_URMA_SDK_FINGERPRINT="${SPARK_URMA_SDK_FINGERPRINT}")
        target_link_libraries(provider_context_test PRIVATE ${URMA_LIBRARY} ${URMA_COMMON_LIBRARY} pthread ${CMAKE_DL_LIBS})
        foreach(api get_rjetty put_rjetty get_seg_ctx put_seg_ctx import_jetty import_seg post_jetty_send_wr poll_jfc)
            target_link_options(provider_context_test PRIVATE "-Wl,--wrap=urma_${api}")
        endforeach()
        add_test(NAME provider_context_test COMMAND provider_context_test)
        set_tests_properties(provider_context_test PROPERTIES LABELS "offline-context" TIMEOUT 15)
    endif()
    foreach(contract local_va_contract_test rm_ctp_dma_lifetime_test rm_ctp_post_ownership_test rm_ctp_mr_import_test rm_ctp_connect_test rm_ctp_init_test registration_diagnostics_test targeted_completion_test)
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
