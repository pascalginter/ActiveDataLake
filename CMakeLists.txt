# -------------------------------------------------------------------------------
# ActiveDataLake
# -------------------------------------------------------------------------------
cmake_minimum_required(VERSION 3.12)
project(ActiveDataLake VERSION 0.0.1)


set(CMAKE_PREFIX_PATH "~/Documents/Universitat/ActiveDataLake/build/vendor/btrblocks/src/btrblocks_src-build/vendor/croaring/lib" ${CMAKE_PREFIX_PATH})


# -------------------------------------------------------------------------------
# Define compile-time flags
# -------------------------------------------------------------------------------

# -------------------------------------------------------------------------------
# C++ build options
# -------------------------------------------------------------------------------
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread -g -O3")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -pthread -g")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -g")
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -g")
add_compile_options(-march=native -Wall -Wextra)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_SOURCE_DIR}/cmake)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include("cmake/clang-tidy.cmake") # Integrate clang-tidy

# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------

include(cmake/avro.cmake)
include(cmake/aws.cmake)
include(cmake/arrow.cmake)
include(cmake/btrblocks.cmake)
include(cmake/croaring.cmake)
include(cmake/oatpp.cmake)
include(cmake/pqxx.cmake)

# ---------------------------------------------------------------------------
# Components
# ---------------------------------------------------------------------------
add_executable(ActiveDataLake src/App.cpp src/virtualization/VirtualizedFile.cpp src/controller/Iceberg/DataController.cpp)
target_link_libraries(ActiveDataLake PRIVATE
        oatpp
        btrblocks
        arrow
        avro
        parquet
        croaring
        libaws-cpp-sdk-core
        libaws-cpp-sdk-s3-crt
        libaws-cpp-sdk-s3
        pqxx
        pq
)

# ---------------------------------------------------------------------------
# Linting
# ---------------------------------------------------------------------------

if (NOT "${lint_targets}" STREQUAL "")
    add_custom_target(lint)
    add_dependencies(lint ${lint_targets})
endif()
